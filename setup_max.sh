#!/bin/bash
set -e
BOT_DIR="/opt/courier-promo-bot"
echo "=== Установка MAX бота ==="

# Copy files
cp courier_core.py "$BOT_DIR/"
cp courier_max.py "$BOT_DIR/"
chown courierbot:courierbot "$BOT_DIR/courier_core.py" "$BOT_DIR/courier_max.py"

# Install deps (no new deps needed — uses requests which is already installed)

# ── systemd service for MAX bot ──
cat > /etc/systemd/system/courier-max-bot.service <<'EOF'
[Unit]
Description=Courier Promo MAX Bot
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=courierbot
WorkingDirectory=/opt/courier-promo-bot
ExecStart=/opt/courier-promo-bot/venv/bin/python3 courier_max.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
StandardOutput=append:/opt/courier-promo-bot/max_bot.log
StandardError=append:/opt/courier-promo-bot/max_bot.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now courier-max-bot
sleep 3
echo "=== Готово ==="
systemctl status courier-max-bot --no-pager
echo ""
echo "Логи MAX: sudo tail -f $BOT_DIR/max_bot.log"
echo ""
echo "Все сервисы:"
systemctl list-units courier-* --no-pager | grep -E "service|timer"
