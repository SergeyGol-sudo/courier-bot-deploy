#!/bin/bash
set -e
BOT_DIR="/opt/courier-promo-bot"
echo "=== Обновление бота v3 (promo usage notifications) ==="
cp "$BOT_DIR/courier_promo_bot.py" "$BOT_DIR/courier_promo_bot.py.bak" 2>/dev/null || true
cp courier_promo_bot.py "$BOT_DIR/"
cp requirements.txt "$BOT_DIR/"
cd "$BOT_DIR" && ./venv/bin/pip install -q -r requirements.txt
chown -R courierbot:courierbot "$BOT_DIR"

# ── systemd timer for check_used (every 3 hours) ──
cat > /etc/systemd/system/courier-check-used.service <<'EOF'
[Unit]
Description=Check used promo codes and notify couriers
After=network.target

[Service]
Type=oneshot
User=courierbot
WorkingDirectory=/opt/courier-promo-bot
ExecStart=/opt/courier-promo-bot/venv/bin/python3 courier_promo_bot.py check_used
Environment=PYTHONUNBUFFERED=1
StandardOutput=append:/opt/courier-promo-bot/check_used.log
StandardError=append:/opt/courier-promo-bot/check_used.log
EOF

cat > /etc/systemd/system/courier-check-used.timer <<'EOF'
[Unit]
Description=Run check_used every 3 hours

[Timer]
OnCalendar=*-*-* 00/3:15:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now courier-check-used.timer
echo "✅ courier-check-used.timer установлен (каждые 3 часа)"

# Restart bot
systemctl restart courier-promo-bot
sleep 3
echo "=== Готово ==="
systemctl status courier-promo-bot --no-pager
echo ""
systemctl list-timers courier-* --no-pager
echo ""
echo "Логи: sudo tail -f $BOT_DIR/bot.log"
echo "check_used логи: sudo tail -f $BOT_DIR/check_used.log"
