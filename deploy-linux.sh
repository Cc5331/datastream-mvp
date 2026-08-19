#!/bin/bash
# =====================================================
# 数据流任务管理系统 - Linux 一键部署脚本
# =====================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_VERSION="1.18.1"
FLINK_DIR="$PROJECT_DIR/flink-1.18.1"
BACKEND_DIR="$PROJECT_DIR/backend"
FRONTEND_DIR="$PROJECT_DIR/frontend"
NODE_VERSION="18"

echo "================================================"
echo "  数据流任务管理系统 - Linux 部署"
echo "  项目目录: $PROJECT_DIR"
echo "================================================"

# 1. 检查 Java
echo ""
echo "[1/5] 检查 Java 17+..."
if command -v java &>/dev/null; then
    JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VER" -ge 17 ]; then
        echo "  ✓ Java 版本: $(java -version 2>&1 | head -1)"
    else
        echo "  ✗ 需要 Java 17+, 当前: $(java -version 2>&1 | head -1)"
        echo "  安装: sudo apt install openjdk-17-jdk  (Ubuntu)"
        echo "        sudo yum install java-17-openjdk (CentOS)"
        exit 1
    fi
else
    echo "  未检测到 Java，请先安装 Java 17+"
    exit 1
fi

# 2. 检查 Node.js
echo ""
echo "[2/5] 检查 Node.js..."
if command -v node &>/dev/null; then
    echo "  ✓ Node.js 版本: $(node --version)"
else
    echo "  ✗ 未检测到 Node.js，自动安装..."
    curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | sudo -E bash -
    sudo apt-get install -y nodejs
    echo "  ✓ Node.js 已安装: $(node --version)"
fi

# 3. 检查/下载 Flink
echo ""
echo "[3/5] 检查 Flink $FLINK_VERSION..."
if [ ! -d "$FLINK_DIR" ]; then
    echo "  正在下载 Flink $FLINK_VERSION..."
    cd "$PROJECT_DIR"
    curl -fSL "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" -o "flink-${FLINK_VERSION}.tgz"
    tar -xzf "flink-${FLINK_VERSION}.tgz"
    rm "flink-${FLINK_VERSION}.tgz"
    echo "  ✓ Flink 下载完成"
else
    echo "  ✓ Flink 已存在"
fi

# 复制已有配置
if [ -f "$PROJECT_DIR/flink/conf/flink-conf.yaml" ]; then
    cp "$PROJECT_DIR/flink/conf/flink-conf.yaml" "$FLINK_DIR/conf/"
    echo "  ✓ 配置文件已复制"
fi

# 4. 编译后端
echo ""
echo "[4/5] 编译后端..."
cd "$BACKEND_DIR"
if [ ! -f "target/mvp-backend-1.0.0.jar" ]; then
    ./mvnw package -DskipTests -q 2>/dev/null || mvn package -DskipTests -q
    echo "  ✓ 编译完成"
else
    echo "  ✓ 已编译"
fi

# 5. 启动服务
echo ""
echo "[5/5] 启动服务..."

# 5.1 启动 Flink 集群
echo "  启动 Flink 集群..."
cd "$FLINK_DIR"
./bin/start-cluster.sh
echo "  ✓ Flink Web UI: http://localhost:8081"

# 5.2 启动后端
echo "  启动后端..."
cd "$BACKEND_DIR"
nohup java -jar target/mvp-backend-1.0.0.jar --spring.profiles.active=linux > app.log 2>&1 &
BACKEND_PID=$!
echo "  ✓ 后端: http://localhost:8080 (PID: $BACKEND_PID)"

# 5.3 启动前端
echo "  启动前端..."
cd "$FRONTEND_DIR"
nohup node serve.js > frontend.log 2>&1 &
FRONTEND_PID=$!
echo "  ✓ 前端: http://localhost:3000 (PID: $FRONTEND_PID)"

# 保存 PID 到文件
echo "$BACKEND_PID" > "$PROJECT_DIR/.backend.pid"
echo "$FRONTEND_PID" > "$PROJECT_DIR/.frontend.pid"

echo ""
echo "================================================"
echo "  部署完成"
echo "================================================"
echo "  前端:    http://localhost:3000"
echo "  后端 API:    http://localhost:8080"
echo "  Flink Web:   http://localhost:8081"
echo "================================================"
echo ""
echo "停止: bash $PROJECT_DIR/stop-linux.sh"
