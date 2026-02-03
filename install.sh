#!/bin/bash

# =================================================================
# RTMP Gateway 一鍵安裝腳本 (適用於 GCE / Ubuntu / Debian)
# =================================================================

# 配置變數 (請根據實際情況修改)
APP_NAME="rtmp-gateway"
BINARY_SOURCE="gateway_unix"
INSTALL_DIR="/opt/rtmp-gateway"
USER_NAME=$(whoami)
SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"

echo "🚀 開始部屬 ${APP_NAME}..."

# 1. 檢查二進制檔案是否存在
if [ ! -f "$BINARY_SOURCE" ]; then
    echo "❌ 錯誤: 找不到編譯好的檔案 $BINARY_SOURCE"
    echo "請先在本地執行 'make build-linux' 並上傳到伺服器。"
    exit 1
fi

# 2. 建立安裝目錄並設定權限
echo "📂 準備安裝目錄: ${INSTALL_DIR}"
sudo mkdir -p ${INSTALL_DIR}
sudo chown ${USER_NAME}:${USER_NAME} ${INSTALL_DIR}

# 3. 移動檔案
cp ${BINARY_SOURCE} ${INSTALL_DIR}/gateway
if [ -f ".env" ]; then
    cp .env ${INSTALL_DIR}/.env
    echo "✅ .env 檔案已同步"
else
    echo "⚠️ 警告: 找不到 .env 檔案，請手動確認 ${INSTALL_DIR}/.env 是否存在"
fi

chmod +x ${INSTALL_DIR}/gateway

# 4. 產生 systemd Service 設定檔
echo "⚙️ 正在產生 systemd 服務設定..."
sudo bash -c "cat > ${SERVICE_FILE}" <<EOF
[Unit]
Description=RTMP Streaming Gateway Service
After=network.target

[Service]
User=${USER_NAME}
Group=${USER_NAME}
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/gateway
Restart=always
RestartSec=5
# 關鍵：自動讀取 .env 變數
EnvironmentFile=${INSTALL_DIR}/.env

[Install]
WantedBy=multi-user.target
EOF

# 5. 啟動服務
echo "🔄 正在啟動服務..."
sudo systemctl daemon-reload
sudo systemctl enable ${APP_NAME}
sudo systemctl restart ${APP_NAME}

echo "------------------------------------------------"
echo "✅ 安裝完成！"
echo "📊 檢查狀態: sudo systemctl status ${APP_NAME}"
echo "📝 查看日誌: sudo journalctl -u ${APP_NAME} -f"
echo "------------------------------------------------"
