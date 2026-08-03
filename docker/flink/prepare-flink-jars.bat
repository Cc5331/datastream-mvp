@echo off
chcp 65001 >nul
cd /d "%~dp0"
set SRC=..\..\flink-1.18.1\lib
set UDF=..\..\udf\target
if not exist lib mkdir lib

echo [1/3] 复制 UDF jar...
copy /y "%UDF%\xml-json-udf-1.0.0.jar" lib\ >nul 2>nul
if not exist lib\xml-json-udf-1.0.0.jar (
    echo [WARN] 未找到 UDF jar，请先构建: cd ..\..\udf ^&^& mvn package -DskipTests
)

echo [2/3] 复制 Flink 连接器 jar...
copy /y "%SRC%\flink-connector-jdbc-3.1.2-1.18.jar" lib\ >nul 2>nul
copy /y "%SRC%\flink-sql-connector-kafka-3.2.0-1.18.jar" lib\ >nul 2>nul
copy /y "%SRC%\mysql-connector-j-9.6.0.jar" lib\ >nul 2>nul
copy /y "%SRC%\mysql-connector-java-8.0.28.jar" lib\ >nul 2>nul

echo [3/3] 当前 docker/flink/lib 内容:
dir /b lib
echo.
echo 完成。可以执行 docker compose up -d --build
