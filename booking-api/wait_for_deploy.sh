#!/bin/bash
for i in {1..30}; do
  if curl -s https://instagram-randevu-bot.onrender.com/version | grep -q "3d158c3"; then
    echo "Deployed successfully!"
    exit 0
  fi
  echo "Waiting..."
  sleep 4
done
echo "Timeout"
