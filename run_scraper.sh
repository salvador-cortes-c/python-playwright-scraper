#!/bin/bash

# ─── Config ───────────────────────────────────────────────────────────────────
DISPLAY_NUM=99
VNC_PORT=5900
NOVNC_PORT=6080
RESOLUTION="1280x800x24"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="/root/.venv"
# ──────────────────────────────────────────────────────────────────────────────

export DISPLAY=:${DISPLAY_NUM}

# ─── Display stack setup ──────────────────────────────────────────────────────
setup_display() {
    echo "🧹 Stopping any existing display processes..."
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

    echo "🌐 Starting noVNC on port ${NOVNC_PORT}..."
    websockify --web=/usr/share/novnc ${NOVNC_PORT} localhost:${VNC_PORT} &
    sleep 1
    if ! pgrep -f websockify > /dev/null; then
        echo "❌ websockify failed to start. Aborting."
        exit 1
    fi
    echo "✅ websockify running."
}

# ─── Check if display stack is already running ────────────────────────────────
if pgrep -x Xvfb > /dev/null && pgrep -x x11vnc > /dev/null && pgrep -f websockify > /dev/null; then
    echo "✅ Display stack already running, skipping setup."
else
    echo "🔧 Display stack not running, setting it up..."
    setup_display
fi

# ─── Activate venv ────────────────────────────────────────────────────────────
if [ ! -f "${VENV_DIR}/bin/activate" ]; then
    echo "❌ Virtual environment not found at ${VENV_DIR}. Aborting."
    exit 1
fi
echo "🐍 Activating virtual environment..."
source "${VENV_DIR}/bin/activate"
echo "✅ Using Python: $(python --version)"

# ─── noVNC reminder ───────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "   👉 Make sure noVNC is open and connected:"
echo "      http://localhost:${NOVNC_PORT}/vnc.html"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ─── Run scraper ──────────────────────────────────────────────────────────────
echo "🚀 Running scraper..."
cd "${PROJECT_DIR}"
python scraper.py "$@"
