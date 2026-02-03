package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/format/rtmp"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	WriteTimeout = 5 * time.Second
	ChannelBuf   = 500 // Number of packets to buffer per destination
)

var logger *zap.Logger

func init() {
	var config zap.Config

    // 根據環境變數決定使用 Production 或 Development 配置
    if os.Getenv("ENV") == "production" {
        config = zap.NewProductionConfig()
    } else {
        config = zap.NewDevelopmentConfig()
    }

    // 手動設定等級（如果環境變數有設 DEBUG=true 就開 Debug）
    if os.Getenv("DEBUG") == "true" {
        config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
    }

    var err error
    logger, err = config.Build()
    if err != nil {
        panic(err)
    }
}

// ---------------------------
// DB & Cache Repositories
// ---------------------------

// StreamInfo contains the quota and destinations for a stream
type StreamInfo struct {
	Key             string    `json:"key"`
	Quota           int64     `json:"quota"`
	DestinationURLs []string  `json:"destination_urls"`
	CachedAt        time.Time `json:"cached_at"`
}

// StreamRepository defines the interface for looking up stream configuration
type StreamRepository interface {
	GetStreamInfo(streamPath string) (*StreamInfo, error)
}

// APIRepository implements StreamRepository backed by an external API
type APIRepository struct {
	baseURL string
	client  *http.Client
}

func NewAPIRepository(baseURL string) *APIRepository {
	return &APIRepository{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (r *APIRepository) GetStreamInfo(path string) (*StreamInfo, error) {
	// Construct API URL: e.g. http://api.internal/stream?key=/live/test
	u, err := url.Parse(r.baseURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("key", path)
	u.RawQuery = q.Encode()

	resp, err := r.client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status: %d", resp.StatusCode)
	}

	var info StreamInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

// RedisRepository acts as L2 Cache: Redis -> Inner(MySQL)
type RedisRepository struct {
	inner  StreamRepository
	client *redis.Client
}

func NewRedisRepository(inner StreamRepository, client *redis.Client) *RedisRepository {
	return &RedisRepository{
		inner:  inner,
		client: client,
	}
}

func (r *RedisRepository) GetStreamInfo(path string) (*StreamInfo, error) {
	ctx := context.Background() // lightweight ctx for redis ops
	infoKey := "info:" + path

	// 1. Check Redis
	val, err := r.client.Get(ctx, infoKey).Result()
	var cacheInfo StreamInfo
	var cacheHit bool

	if err == nil {
		if jsonErr := json.Unmarshal([]byte(val), &cacheInfo); jsonErr == nil {
			cacheHit = true
			if time.Since(cacheInfo.CachedAt) < 10*time.Minute {
				logger.Debug("L2 Redis Cache hit (Fresh)", zap.String("path", path))
				return &cacheInfo, nil
			}
			logger.Warn("L2 Redis Cache hit (Stale), attempting refresh", zap.String("path", path))
		}
	} else if err != redis.Nil {
		logger.Warn("Redis get error", zap.Error(err))
	}

	// 2. Fetch from Inner (API)
	info, fetchErr := r.inner.GetStreamInfo(path)

	// SWR Resilience Logic
	if fetchErr != nil {
		if cacheHit {
			logger.Warn("L3 API Failed, serving stale data", zap.String("path", path), zap.Error(fetchErr))
			return &cacheInfo, nil
		}
		return nil, fetchErr
	}

	// 3. Cache in Redis (Refresh)
	info.CachedAt = time.Now()

	// Physical TTL: 24h (Longer than logical TTL 10m to allow SWR)
	ttl := 24 * time.Hour
	if len(info.DestinationURLs) == 0 {
		ttl = 10 * time.Minute // Shorter for empty results
	}

	if data, err := json.Marshal(info); err == nil {
		if err := r.client.Set(ctx, infoKey, data, ttl).Err(); err != nil {
			logger.Warn("Redis set error", zap.Error(err))
		} else {
			logger.Debug("L2 Redis populated", zap.String("path", path))
		}
	}

	return info, nil
}

// CachedRepository decorates a StreamRepository with in-memory caching (L1)
type CachedRepository struct {
	inner StreamRepository
	cache *cache.Cache
}

func NewCachedRepository(inner StreamRepository, defaultExpiration, cleanupInterval time.Duration) *CachedRepository {
	return &CachedRepository{
		inner: inner,
		cache: cache.New(defaultExpiration, cleanupInterval),
	}
}

func (r *CachedRepository) GetStreamInfo(path string) (*StreamInfo, error) {
	// L1 Cache Logic
	if val, found := r.cache.Get(path); found {
		info := val.(*StreamInfo)
		if len(info.DestinationURLs) == 0 {
			logger.Debug("L1 Local Cache hit (bloomed empty)", zap.String("path", path))
		} else {
			logger.Debug("L1 Local Cache hit", zap.String("path", path))
		}
		return info, nil
	}

	// Fallback to L2 (Redis) -> L3 (API)
	info, err := r.inner.GetStreamInfo(path)
	if err != nil {
		return nil, err
	}

	// Cache Result in L1
	if len(info.DestinationURLs) > 0 {
		r.cache.Set(path, info, cache.DefaultExpiration)
		logger.Debug("L1 Local Cache populated", zap.String("path", path))
	} else {
		r.cache.Set(path, info, 30*time.Second)
		logger.Debug("L1 Local Cache populated (bloomed empty)", zap.String("path", path))
	}

	return info, nil
}

func (r *CachedRepository) Invalidate(path string) {
	r.cache.Delete(path)
	logger.Info("L1 Local Cache invalidated", zap.String("path", path))
}

// Global repo instance
var streamRepo *CachedRepository
var globalRedis *redis.Client

// ---------------------------
// Traffic Logging
// ---------------------------

type TrafficTracker struct {
	streamKey   string
	bytes       int64
	mu          sync.Mutex
	redisClient *redis.Client
	lastFlush   time.Time
}

func NewTrafficTracker(streamKey string, rdb *redis.Client) *TrafficTracker {
	return &TrafficTracker{
		streamKey:   streamKey,
		redisClient: rdb,
		lastFlush:   time.Now(),
	}
}

func (t *TrafficTracker) Add(n int) {
	t.mu.Lock()
	t.bytes += int64(n)
	t.mu.Unlock()
}

func (t *TrafficTracker) Flush() {
	t.mu.Lock()
	bytes := t.bytes
	t.bytes = 0
	// lastFlush := t.lastFlush // Unused if we don't log to DB
	now := time.Now()
	t.lastFlush = now
	t.mu.Unlock()

	if bytes == 0 {
		return
	}

	// Redis: Incremental (Monthly)
	// Key: usage:/live/test
	usageKey := fmt.Sprintf("usage:%s", t.streamKey)
	ctx := context.Background()

	pipe := t.redisClient.Pipeline()
	pipe.IncrBy(ctx, usageKey, bytes)
	pipe.Expire(ctx, usageKey, 60*24*time.Hour) // Keep for 60 days
	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error("Failed to update Redis traffic", zap.Error(err))
	}
}

func (t *TrafficTracker) StartTicker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Flush() // Final flush
			return
		case <-ticker.C:
			t.Flush()
		}
	}
}

// ---------------------------
// Existing Gateway Logic
// ---------------------------

// AsyncPacketWriter wraps an av.PacketWriter with a channel to ensure non-blocking writes (up to buffer).
type AsyncPacketWriter struct {
	url        string
	writer     av.PacketWriter
	pktCh      chan av.Packet
	ctx        context.Context
	cancel     context.CancelFunc
	closed     bool
	mu         sync.Mutex
	done       chan struct{}

	// Metrics
	dropCount uint64
	sentCount uint64
}

// Global active stream counter
var activeStreams int64

func NewAsyncPacketWriter(ctx context.Context, apiConn av.PacketWriter, url string) *AsyncPacketWriter {
	ctx, cancel := context.WithCancel(ctx)
	w := &AsyncPacketWriter{
		url:    url,
		writer: apiConn,
		pktCh:  make(chan av.Packet, ChannelBuf),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go w.loop()
	return w
}

func (w *AsyncPacketWriter) loop() {
	defer close(w.done)
	defer w.cancel() // Ensure we cleanup if loop exits

	for {
		select {
		case <-w.ctx.Done():
			return
		case pkt := <-w.pktCh:
			// Write with basic safety
			if conn, ok := w.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
				conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			}
			if err := w.writer.WritePacket(pkt); err != nil {
				logger.Error("Error writing packet", zap.String("url", w.url), zap.Error(err))
				return
			}
			w.sentCount++
		}
	}
}

func (w *AsyncPacketWriter) WritePacket(pkt av.Packet) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("writer closed")
	}

	select {
	case w.pktCh <- pkt:
		return nil
	default:
		if pkt.IsKeyFrame {
			select {
			case <-w.pktCh:
			default:
			}
			select {
			case w.pktCh <- pkt:
				w.dropCount++
				return nil
			default:
				w.dropCount++
				return nil
			}
		}
		w.dropCount++
		return nil
	}
}

func (w *AsyncPacketWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cancel()

	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		logger.Warn("Writer close timed out, forcing close", zap.String("url", w.url))
	}

	if c, ok := w.writer.(interface{ Close() error }); ok {
		c.Close()
	}

	logger.Info("Closed writer",
		zap.String("url", w.url),
		zap.Uint64("sent", w.sentCount),
		zap.Uint64("dropped", w.dropCount),
	)
	return nil
}

// Broadcaster distributes packets to multiple writers
type Broadcaster struct {
	writers []*AsyncPacketWriter
}

func (b *Broadcaster) WritePacket(pkt av.Packet) error {
	for _, w := range b.writers {
		_ = w.WritePacket(pkt)
	}
	return nil
}

// Protocol interfaces for clarity
type RTMPMuxer interface {
	av.PacketWriter
	WriteHeader(streams []av.CodecData) error
	Close() error
}

// DialRTMP supports both rtmp:// and rtmps://
func DialRTMP(urlStr string) (RTMPMuxer, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	if u.Scheme == "rtmps" || u.Port() == "443" {
		host := u.Hostname()
		port := u.Port()
		if port == "" {
			port = "443"
		}
		addr := fmt.Sprintf("%s:%s", host, port)

		conf := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         host, // SNI Support
		}
		conn, err = tls.Dial("tcp", addr, conf)
	} else {
		host := u.Hostname()
		port := u.Port()
		if port == "" {
			port = "1935"
		}
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
	}

	if err != nil {
		return nil, err
	}

	c := rtmp.NewConn(conn)
	c.URL = u
	return c, nil
}

func startStreamForwarding(conn *rtmp.Conn) {
	atomic.AddInt64(&activeStreams, 1)
	defer atomic.AddInt64(&activeStreams, -1)

	logger.Info("New connection", zap.String("path", conn.URL.Path))
	defer conn.Close()

	// 1. Resolve Destinations using Repository (MySQL + Redis + Cache)
	info, err := streamRepo.GetStreamInfo(conn.URL.Path)
	if err != nil {
		logger.Error("DB/Cache Lookup Failed", zap.Error(err))
		return
	}
	if len(info.DestinationURLs) == 0 {
		logger.Warn("No mapping found", zap.String("path", conn.URL.Path))
		return
	}

	// 1.5 Quota Check
	// Key: usage:/live/test
	usageKey := fmt.Sprintf("usage:%s", conn.URL.Path)
	val, err := globalRedis.Get(context.Background(), usageKey).Result()

	var currentUsage int64
	if err == nil {
		if v, parseErr := strconv.ParseInt(strings.TrimSpace(val), 10, 64); parseErr == nil {
			currentUsage = v
		} else {
			logger.Error("Failed to parse quota usage", zap.Error(parseErr))
		}
	} else if err != redis.Nil {
		logger.Error("Failed to check quota usage", zap.Error(err))
	}

	// 10% tolerance error margin (User Request: currentUsage * 1.1)
	if float64(currentUsage)*1.1 >= float64(info.Quota) {
		logger.Warn("Stream quota exceeded",
			zap.String("path", conn.URL.Path),
			zap.Int64("usage", currentUsage),
			zap.Int64("quota", info.Quota))
		return
	}

	destUrls := info.DestinationURLs

	var writers []*AsyncPacketWriter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Traffic Tracker
	tracker := NewTrafficTracker(conn.URL.Path, globalRedis)
	go tracker.StartTicker(ctx, 10*time.Second)

	// 2. Prepare Headers (Streams)
	streams, err := conn.Streams()
	if err != nil {
		logger.Error("Failed to read streams from source", zap.Error(err))
		return
	}

	// 3. Connect to all destinations
	for _, destURL := range destUrls {
		logger.Info("Dialing destination", zap.String("url", destURL))

		dstConn, err := DialRTMP(destURL)
		if err != nil {
			logger.Error("Failed to dial", zap.String("url", destURL), zap.Error(err))
			continue
		}

		if err := dstConn.WriteHeader(streams); err != nil {
			logger.Error("Failed to write header", zap.String("url", destURL), zap.Error(err))
			dstConn.Close()
			continue
		}

		w := NewAsyncPacketWriter(ctx, dstConn, destURL)
		writers = append(writers, w)
	}

	if len(writers) == 0 {
		logger.Warn("No valid destinations connected. Stopping.")
		return
	}

	broadcaster := &Broadcaster{writers: writers}

	// 4. Forward Loop
	logger.Info("Starting packet forwarding")

	for {
		pkt, err := conn.ReadPacket()
		if err != nil {
			if err.Error() != "EOF" {
				logger.Error("Read error", zap.Error(err))
			}
			break
		}

		// Count Traffic (Async Flush)
		payloadLen := len(pkt.Data)
		tracker.Add(payloadLen)

		// Real-time Quota Check (Memory-based)
		currentUsage += int64(payloadLen)
		if float64(currentUsage)*1.1 >= float64(info.Quota) {
			logger.Warn("Stream quota exceeded (runtime)",
				zap.String("path", conn.URL.Path),
				zap.Int64("usage", currentUsage),
				zap.Int64("quota", info.Quota))
			break
		}

		broadcaster.WritePacket(pkt)
	}

	logger.Info("Source stream finished. Closing forwarders.")

	for _, w := range writers {
		w.Close()
	}
}

func main() {
	defer logger.Sync()

	apiURL := os.Getenv("STREAM_API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:8081/mock_l3"
		logger.Info("Using default Mock API URL", zap.String("url", apiURL))
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	globalRedis = rdb

	// Chain: Local(L1) -> Redis(L2) -> API(L3)
	apiRepo := NewAPIRepository(apiURL)
	redisRepo := NewRedisRepository(apiRepo, rdb)
	streamRepo = NewCachedRepository(redisRepo, 5*time.Minute, 10*time.Minute)

	logger.Info("Stream Repository Initialized (API + Redis + LocalCache)")

	// 2. Start Redis Pub/Sub Listener
	go func() {
		pubsub := rdb.Subscribe(context.Background(), "stream_updates")
		defer pubsub.Close()

		ch := pubsub.Channel()
		logger.Info("Subscribed to Redis active invalidation channel", zap.String("channel", "stream_updates"))

		for msg := range ch {
			path := msg.Payload
			logger.Info("Received invalidation signal", zap.String("path", path))
			streamRepo.Invalidate(path)
		}
	}()

	// 3. Start Health Check Server + Sim Update API
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			count := atomic.LoadInt64(&activeStreams)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK. Active Streams: %d\n", count)
		})

		// Helper to simulate DB update and broadcast
		http.HandleFunc("/api/sim_update", func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Query().Get("path")
			if path == "" {
				http.Error(w, "missing path", http.StatusBadRequest)
				return
			}
			// Publish to Redis
			err := rdb.Publish(context.Background(), "stream_updates", path).Err()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "Published invalidation for: %s", path)
		})

		// Mock L3 API
		http.HandleFunc("/mock_l3", func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			if key == "" {
				http.Error(w, "missing key", http.StatusBadRequest)
				return
			}

			// Sample Response
			resp := StreamInfo{
				Key:   key,
				Quota: 1024 * 1024 * 50, // 50MB
				DestinationURLs: []string{
					"rtmps://252656b2b64e.global-contribute.live-video.net:443/app/sk_ap-northeast-1_SpghWWQG9Hms_17UHQatYdcsaVGeyczjsfMfEcdagCU",
					"rtmps://252656b2b64e.global-contribute.live-video.net:443/app/sk_ap-northeast-1_uYwVq6KdDCjd_5cWUXRqFa04dF5pE4d9WEtPUFTLufb",
				},
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		})

		logger.Info("Health check & Admin API listening", zap.String("addr", ":8081"))
		if err := http.ListenAndServe(":8081", nil); err != nil {
			logger.Error("Health check server failed", zap.Error(err))
		}
	}()

	addr := ":1935"
	server := &rtmp.Server{
		Addr: addr,
	}

	server.HandlePublish = func(conn *rtmp.Conn) {
		startStreamForwarding(conn)
	}

	logger.Info("RTMP Gateway (Async/Multi-Dest/DB-Redis-Map) listening", zap.String("addr", addr))
	if err := server.ListenAndServe(); err != nil {
		logger.Fatal("Server failed", zap.Error(err))
	}
}
