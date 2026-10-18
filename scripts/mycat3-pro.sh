#!/usr/bin/env bash
# Mycat3 Pro 启动/停止/状态脚本
# 用法: ./scripts/mycat3-pro.sh {start|stop|restart|status} [home_dir]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
HOME_DIR="${2:-${MYCAT3_PRO_HOME:-$BASE_DIR/conf}}"
BIN="${MYCAT3_PRO_BIN:-$BASE_DIR/target/release/mycat3-pro}"
PID_FILE="$HOME_DIR/mycat3-pro.pid"
LOG_FILE="$HOME_DIR/logs/mycat3-pro.log"

start() {
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "Mycat3 Pro already running (pid $(cat "$PID_FILE"))"
        exit 1
    fi
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "Starting Mycat3 Pro (home=$HOME_DIR) ..."
    MYCAT3_PRO_HOME="$HOME_DIR" nohup "$BIN" start --home "$HOME_DIR" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 1
    if kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "Mycat3 Pro started (pid $(cat "$PID_FILE")), log: $LOG_FILE"
    else
        echo "Failed to start, see $LOG_FILE"
        exit 1
    fi
}

stop() {
    if [[ -f "$PID_FILE" ]]; then
        local pid; pid="$(cat "$PID_FILE")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            for _ in $(seq 1 30); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.5
            done
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" || true
        fi
        rm -f "$PID_FILE"
        echo "Mycat3 Pro stopped"
    else
        echo "Mycat3 Pro is not running (no pid file)"
    fi
}

status() {
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "Mycat3 Pro running (pid $(cat "$PID_FILE"))"
    else
        echo "Mycat3 Pro not running"
    fi
}

case "${1:-}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; start ;;
    status)  status ;;
    *) echo "Usage: $0 {start|stop|restart|status} [home_dir]"; exit 1 ;;
esac
