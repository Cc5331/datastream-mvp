#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "??????..."

if [ -f "$PROJECT_DIR/.backend.pid" ]; then
    kill $(cat "$PROJECT_DIR/.backend.pid") 2>/dev/null && echo "  ? ?????"
    rm "$PROJECT_DIR/.backend.pid"
fi
if [ -f "$PROJECT_DIR/.frontend.pid" ]; then
    kill $(cat "$PROJECT_DIR/.frontend.pid") 2>/dev/null && echo "  ? ?????"
    rm "$PROJECT_DIR/.frontend.pid"
fi

if [ -d "$PROJECT_DIR/flink-1.18.1" ]; then
    "$PROJECT_DIR/flink-1.18.1/bin/stop-cluster.sh" 2>/dev/null && echo "  ? Flink ?????"
fi

echo "???????"
