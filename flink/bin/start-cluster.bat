@echo off
chcp 65001 >nul
REM ==========================================
REM  启动 Flink Standalone 集群 (通过 Git Bash)
REM ==========================================

set FLINK_DIR=D:\code\flink-1.18.1
set BASH="C:\Program Files\Git\bin\bash.exe"

echo ============================================
echo  🚀 启动 Flink Standalone 集群
echo ============================================
echo  Flink 目录: %FLINK_DIR%
echo  Web UI:     http://localhost:8081
echo ============================================

REM 进入 Flink 目录并用 Git Bash 启动集群
cd /d %FLINK_DIR%
%BASH% -c "./bin/start-cluster.sh"

echo.
echo  ✅ Flink 集群启动完成!
echo  📊 Web UI: http://localhost:8081
echo  🛑 停止:   stop-cluster.bat
echo ============================================
pause
