#!/bin/bash
set -e

# Kill any process already using port 8000
echo "🔍 Checking for processes on port 8000..."
PID=$(lsof -ti:8000 || true)
if [ -n "$PID" ]; then
    echo "⚠️  Killing process on port 8000 (PID: $PID)..."
    kill -9 $PID
fi

# Kill any ngrok process running
NGROK_PID=$(pgrep ngrok || true)
if [ -n "$NGROK_PID" ]; then
    echo "⚠️  Killing existing ngrok process (PID: $NGROK_PID)..."
    kill -9 $NGROK_PID
fi

# Start Python app in background
echo "🚀 Starting Python app..."
python3 app.py &

# Wait for the app to boot
sleep 2

# Start ngrok tunnel to port 8000
echo "🌐 Starting ngrok tunnel..."
ngrok http 8000 > /dev/null &

# Give ngrok time to start
sleep 2

# Print public URL from ngrok's API
if curl --silent http://127.0.0.1:4040/api/tunnels &>/dev/null; then
    PUBLIC_URL=$(curl -s http://127.0.0.1:4040/api/tunnels | grep -oE "https://[a-z0-9]+\.ngrok-free\.app" | head -n 1)
    echo ""
    echo "✅ App is running at: http://127.0.0.1:8000"
    echo "🌍 Public ngrok URL: $PUBLIC_URL"
    echo ""
else
    echo "❌ Could not retrieve ngrok public URL. Is ngrok installed?"
fi
