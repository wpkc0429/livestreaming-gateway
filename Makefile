# 參數設定
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
BINARY_NAME=gateway
BINARY_UNIX=$(BINARY_NAME)_unix
MAIN_FILE=main.go

# 宣告偽目標，防止與檔名衝突
.PHONY: all build build-linux clean run run-env test deps

all: deps test build

build:
	$(GOBUILD) -o $(BINARY_NAME) $(MAIN_FILE)

# 針對 GCE (Linux) 的跨平台編譯，優化了 LDFLAGS 以減小檔案體積
build-linux:
	@echo "Building for Linux..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags="-s -w" -o $(BINARY_UNIX) $(MAIN_FILE)

# 執行測試
test:
	$(GOCMD) test -v ./...

# 安裝依賴並整理 mod 檔案
deps:
	$(GOCMD) mod tidy
	$(GOCMD) mod download

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)

# 直接運行 (本地開發用)
run:
	$(GOBUILD) -o $(BINARY_NAME) $(MAIN_FILE)
	./$(BINARY_NAME)

# 自動讀取 .env 並運行
run-env: build
	@echo "Starting with .env configuration..."
	@set -a; [ -f .env ] && . ./.env; set +a; ./$(BINARY_NAME)