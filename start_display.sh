#!/bin/bash

# ─── Config ───────────────────────────────────────────────────────────────────
DISPLAY_NUM=99
VNC_PORT=5900
NOVNC_PORT=6080
RESOLUTION="1280x800x24"
# ──────────────────────────────────────────────────────────────────────────────

echo "🧹 Stopping any existing processes..."
pkill Xvfb 2>/dev/null
pkill x11vnc 2>/dev/null
pkill websockify 2>/dev/null
sleep 1

echo "🖥️  Clearing shared memory..."
rm -rf /dev/shm/* 2>/dev/null

echo "🖥️  Starting Xvfb on :${DISPLAY_NUM}..."
Xvfb :${DISPLAY_NUM} -screen 0 ${RESOLUTION} &
sleep 1

if ! pgrep -x Xvfb > /dev/null; then
    echo "❌ Xvfb failed to start. Aborting."
    exit 1
fi
echo "✅ Xvfb running."

echo "📡 Starting x11vnc..."
x11vnc -display :${DISPLAY_NUM} -nopw -listen localhost -xkb -forever -noshm &
sleep 1

if ! pgrep -x x11vnc > /dev/null; then
    echo "❌ x11vnc failed to start. Aborting."
    exit 1
fi
echo "✅ x11vnc running."

echo "🌐 Starting noVNC (websockify) on port ${NOVNC_PORT}..."
websockify --web=/usr/share/novnc ${NOVNC_PORT} localhost:${VNC_PORT} &
sleep 1

if ! pgrep -f websockify > /dev/null; then
    echo "❌ websockify failed to start. Aborting."
    exit 1
fi
echo "✅ websockify running."

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Display stack is ready!"
echo ""
echo "   1. Open in code-server's built-in browser:"
echo "      http://localhost:${NOVNC_PORT}/vnc.html"
echo ""
echo "   2. Click 'Connect' in the noVNC page"
echo ""
echo "   3. In a new terminal tab, run your scraper:"
echo "      export DISPLAY=:${DISPLAY_NUM}"
echo "      python scraper.py --headed ..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
