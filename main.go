package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/format/rtmp"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	_ "github.com/go-sql-driver/mysql"
)

const (
	WriteTimeout = 5 * time.Second
	ChannelBuf   = 500 // Number of packets to buffer per destination
)

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
}

// ---------------------------
// DB & Cache Repositories
// ---------------------------

// StreamRepository defines the interface for looking up stream destinations
type StreamRepository interface {
	GetDestinations(streamPath string) ([]string, error)
}

// MySQLRepository implements StreamRepository backed by a MySQL database provided by user.
// Table Schema (Assumed):
// CREATE TABLE stream_mappings (
//   id INT AUTO_INCREMENT PRIMARY KEY,
//   stream_key VARCHAR(255) NOT NULL, -- e.g. "/live/test"
//   destination_url TEXT NOT NULL     -- e.g. "rtmp://target/app/key"
// );
type MySQLRepository struct {
	db *sql.DB
}

func NewMySQLRepository(dsn string) (*MySQLRepository, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &MySQLRepository{db: db}, nil
}

func (r *MySQLRepository) GetDestinations(path string) ([]string, error) {
	// Query DB for all destinations for this path
	rows, err := r.db.Query("SELECT destination_url FROM stream_mappings WHERE stream_key = ?", path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dests []string
	for rows.Next() {
		var dest string
		if err := rows.Scan(&dest); err != nil {
			return nil, err
		}
		dests = append(dests, dest)
	}
	return dests, nil
}

// RedisRepository acts as L2 Cache: Redis -> Inner(MySQL)
type RedisRepository struct {
	inner  StreamRepository
	client *redis.Client
}

func NewRedisRepository(inner StreamRepository, addr string) *RedisRepository {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisRepository{
		inner:  inner,
		client: rdb,
	}
}

func (r *RedisRepository) GetDestinations(path string) ([]string, error) {
	ctx := context.Background() // lightweight ctx for redis ops

	// 1. Check Redis
	val, err := r.client.Get(ctx, path).Result()
	if err == nil {
		var dests []string
		if jsonErr := json.Unmarshal([]byte(val), &dests); jsonErr == nil {
			logger.Debug("L2 Redis Cache hit", zap.String("path", path))
			return dests, nil
		}
	} else if err != redis.Nil {
		logger.Warn("Redis get error", zap.Error(err))
	}

	// 2. Fallback to Inner (MySQL)
	dests, err := r.inner.GetDestinations(path)
	if err != nil {
		return nil, err
	}

	// 3. Cache in Redis
	// Cache valid results for 10 minutes.
	// Empty results can also be cached if we want global protection.
	ttl := 10 * time.Minute
	if len(dests) == 0 {
		ttl = 1 * time.Minute // Shorter TTL for empty results
	}

	if data, err := json.Marshal(dests); err == nil {
		if err := r.client.Set(ctx, path, data, ttl).Err(); err != nil {
			logger.Warn("Redis set error", zap.Error(err))
		} else {
			logger.Debug("L2 Redis populated", zap.String("path", path))
		}
	}

	return dests, nil
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

func (r *CachedRepository) GetDestinations(path string) ([]string, error) {
	// L1 Cache Logic
	if val, found := r.cache.Get(path); found {
		dests := val.([]string)
		if len(dests) == 0 {
			logger.Debug("L1 Local Cache hit (bloomed empty)", zap.String("path", path))
		} else {
			logger.Debug("L1 Local Cache hit", zap.String("path", path))
		}
		return dests, nil
	}

	// Fallback to L2 (Redis) -> L3 (MySQL)
	dests, err := r.inner.GetDestinations(path)
	if err != nil {
		return nil, err
	}

	// Cache Result in L1
	if len(dests) > 0 {
		r.cache.Set(path, dests, cache.DefaultExpiration)
		logger.Debug("L1 Local Cache populated", zap.String("path", path))
	} else {
		r.cache.Set(path, dests, 30*time.Second)
		logger.Debug("L1 Local Cache populated (bloomed empty)", zap.String("path", path))
	}

	return dests, nil
}

// Global repo instance
var streamRepo StreamRepository

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
	destUrls, err := streamRepo.GetDestinations(conn.URL.Path)
	if err != nil {
		logger.Error("DB/Cache Lookup Failed", zap.Error(err))
		return
	}
	if len(destUrls) == 0 {
		logger.Warn("No mapping found", zap.String("path", conn.URL.Path))
		return
	}

	var writers []*AsyncPacketWriter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

		broadcaster.WritePacket(pkt)
	}

	logger.Info("Source stream finished. Closing forwarders.")

	for _, w := range writers {
		w.Close()
	}
}

func main() {
	defer logger.Sync()

	// 1. Initialize DB + Cache Layering
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		logger.Fatal("MYSQL_DSN environment variable is required")
	}

	mysqlRepo, err := NewMySQLRepository(dsn)
	if err != nil {
		logger.Fatal("Failed to connect to MySQL", zap.Error(err))
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// Chain: Local(L1) -> Redis(L2) -> MySQL(L3)
	redisRepo := NewRedisRepository(mysqlRepo, redisAddr)
	streamRepo = NewCachedRepository(redisRepo, 5*time.Minute, 10*time.Minute)

	logger.Info("Stream Repository Initialized (MySQL + Redis + LocalCache)")

	// 2. Start Health Check Server
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			count := atomic.LoadInt64(&activeStreams)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK. Active Streams: %d\n", count)
		})
		logger.Info("Health check listening", zap.String("addr", ":8081"))
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
