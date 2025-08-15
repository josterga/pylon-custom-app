#!/bin/bash
set -e

echo "🛑 Stopping app and ngrok..."

# Kill Python app on port 8000
PID=$(lsof -ti:8000 || true)
if [ -n "$PID" ]; then
    echo "Killing Python app (PID: $PID)"
    kill -9 $PID
fi

# Kill ngrok process
NGROK_PID=$(pgrep ngrok || true)
if [ -n "$NGROK_PID" ]; then
    echo "Killing ngrok (PID: $NGROK_PID)"
    kill -9 $NGROK_PID
fi

echo "✅ All processes stopped."
