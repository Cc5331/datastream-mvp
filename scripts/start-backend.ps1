<#
.SYNOPSIS
    启动后端（独立进程，自动注入根目录 .env），供人工启动与看门狗（watchdog.ps1）复用。

.DESCRIPTION
    与 start-all.bat 的后端步骤保持一致：
      - 从根目录 .env 注入 MYSQL_*/JWT_SECRET/DEEPSEEK_*/SMTP_* 等环境变量（缺失 JWT_SECRET 会直接失败）
      - 设置 SERVER_PORT / FLINK_CLUSTER_PORT / FLINK_SQL_GATEWAY_PORT（本机 Flink 是 18081/18083）
      - 以 Start-Process 独立进程启动，不随调用方 shell 退出而被杀
    启动后轮询端口直到就绪，返回进程对象。

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\start-backend.ps1
    powershell -ExecutionPolicy Bypass -File scripts\start-backend.ps1 -Port 18080 -WaitSeconds 90
#>
param(
    [int]$Port = 18080,
    [int]$FlinkPort = 18081,
    [int]$SqlGatewayPort = 18083,
    [string]$Jar = "",
    [string]$MaxHeap = "1536m",
    [int]$WaitSeconds = 90
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if (-not $Jar) { $Jar = Join-Path $root 'backend\target\mvp-backend-1.0.0.jar' }

if (-not (Test-Path $Jar)) {
    throw "找不到后端 jar：$Jar`n请先构建：cd backend; mvn -DskipTests package"
}

# 注入根目录 .env（后端强依赖 JWT_SECRET；MySQL/AI/SMTP 等也来自这里）
$envFile = Join-Path $root '.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$') {
            Set-Item -Path ("env:" + $matches[1]) -Value $matches[2].Trim()
        }
    }
}
if (-not $env:JWT_SECRET) {
    throw 'JWT_SECRET 未配置：后端启动会直接失败（JwtUtil 要求至少 32 字节）。请在根目录 .env 中设置。'
}

Set-Item -Path 'env:SERVER_PORT' -Value "$Port"
Set-Item -Path 'env:FLINK_CLUSTER_PORT' -Value "$FlinkPort"
Set-Item -Path 'env:FLINK_SQL_GATEWAY_PORT' -Value "$SqlGatewayPort"

$java = (Get-Command java -ErrorAction SilentlyContinue).Source
if (-not $java) { throw 'PATH 中找不到 java，请确认已安装 JDK 17 并加入 PATH。' }

$proc = Start-Process -FilePath $java `
    -ArgumentList @("-Xmx$MaxHeap", '-jar', $Jar) `
    -WorkingDirectory (Join-Path $root 'backend') `
    -WindowStyle Minimized -PassThru

Write-Output "后端已启动（PID=$($proc.Id)），等待端口 $Port 就绪…"
$deadline = (Get-Date).AddSeconds($WaitSeconds)
$ready = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 2
    if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { $ready = $true; break }
    if ($proc.HasExited) { throw "后端进程启动后立即退出（exit=$($proc.ExitCode)），请查看 backend\app.log" }
}

if ($ready) {
    Write-Output "就绪：http://localhost:$Port（健康探针 /api/health）"
    exit 0
}
throw "等待 $WaitSeconds 秒后端口 $Port 仍未就绪，请查看 backend\app.log"
