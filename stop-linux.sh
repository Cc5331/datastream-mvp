#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "正在停止服务..."

if [ -f "$PROJECT_DIR/.backend.pid" ]; then
    kill $(cat "$PROJECT_DIR/.backend.pid") 2>/dev/null && echo "  ✓ 后端已停止"
    rm "$PROJECT_DIR/.backend.pid"
fi
if [ -f "$PROJECT_DIR/.frontend.pid" ]; then
    kill $(cat "$PROJECT_DIR/.frontend.pid") 2>/dev/null && echo "  ✓ 前端已停止"
    rm "$PROJECT_DIR/.frontend.pid"
fi

if [ -d "$PROJECT_DIR/flink-1.18.1" ]; then
    "$PROJECT_DIR/flink-1.18.1/bin/stop-cluster.sh" 2>/dev/null && echo "  ✓ Flink 集群已停止"
fi

echo "全部已停止"
