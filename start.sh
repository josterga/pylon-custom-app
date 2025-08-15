#!/bin/bash
set -e

# Start supervisord in background
supervisord -c /etc/supervisor/conf.d/supervisord.conf &

# Wait for ngrok to be up
echo "Waiting for ngrok to start..."
until curl --silent --max-time 2 http://localhost:4040/api/tunnels > /dev/null; do
  sleep 1
done

# Extract the public URL
PUBLIC_URL=$(curl --silent http://localhost:4040/api/tunnels \
  | grep -oE "https://[a-z0-9]+\.ngrok-free\.app" | head -n 1)

if [ -n "$PUBLIC_URL" ]; then
  echo ""
  echo "🚀 Your public ngrok URL: $PUBLIC_URL"
  echo ""
else
  echo "❌ Could not detect ngrok public URL."
fi

# Keep container running (supervisord will manage processes)
wait
