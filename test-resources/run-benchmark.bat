@echo off
chcp 65001 >nul
echo ============================================
echo  通用流处理任务管理平台 - MVP 基准测试
echo  硬件: 8vCPU / 32G RAM / 300G HDD
echo ============================================
echo.
echo 测试场景:
echo   - CSV 输入 -^> 字段拼接 -^> CSV 输出
echo   - 数据量: 10万 / 100万 / 1000万 行
echo   - 并行度: 1 / 2 / 4 / 8
echo.
echo 开始测试...
echo.

python benchmark.py

echo.
echo 测试完成! 请查看 test-results/ 目录下的报告。
pause
