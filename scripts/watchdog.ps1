<#
.SYNOPSIS
    后端健康探针 + 看门狗：定期探测 /api/health，连续失败则自动重启后端并写日志。

.DESCRIPTION
    为什么不能只探端口：2026-09-16 出现过「18080 端口在监听、TCP 能连上，但任何请求都不响应」
    的僵死（工作线程全空闲、CPU 零增长、连接堆积在 CLOSE_WAIT），此时浏览器登录会一直转圈。
    本脚本走完整 HTTP 链路探测（/api/health 还会真查一次数据库），因此能抓住这类故障。

    行为：
      - 每 IntervalSeconds 秒探测一次，记录耗时；连续 FailureThreshold 次失败 → 判定不健康
      - 判定不健康后：停止占用端口的进程 → 调 scripts\start-backend.ps1 重新拉起 → 复检
      - 重启保护：两次重启间隔不小于 RestartCooldownSeconds，且每小时最多 MaxRestartsPerHour 次
      - 全过程追加写日志（默认 logs\watchdog.log，超过 5MB 自动滚动）

.PARAMETER Once
    只探测一次：健康退出码 0，不健康退出码 1（适合放到 CI / 演示前检查）

.PARAMETER Preflight
    演示前预检：一次性检查 前端/后端/Flink/SQL Gateway/Kafka 五个服务 + 健康探针，打印表格

.PARAMETER NoRestart
    只告警不重启（观察模式，用于确认故障是否真实存在）

.EXAMPLE
    # 常驻看门狗（默认 30s 一次，连续 3 次失败自动重启）
    powershell -ExecutionPolicy Bypass -File scripts\watchdog.ps1

    # 演示前预检（一次性）
    powershell -ExecutionPolicy Bypass -File scripts\watchdog.ps1 -Preflight

    # 观察模式跑 10 次探针，不重启
    powershell -ExecutionPolicy Bypass -File scripts\watchdog.ps1 -MaxProbes 10 -NoRestart
#>
param(
    [string]$BaseUrl = "http://127.0.0.1:18080",
    [int]$Port = 18080,
    [int]$IntervalSeconds = 30,
    [int]$TimeoutSeconds = 5,
    [int]$FailureThreshold = 3,
    [int]$RestartCooldownSeconds = 60,
    [int]$MaxRestartsPerHour = 5,
    [int]$MaxProbes = 0,          # 0 = 一直跑
    [string]$LogFile = "",
    [switch]$Once,
    [switch]$Preflight,
    [switch]$NoRestart
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if (-not $LogFile) {
    $logDir = Join-Path $root 'logs'
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
    $LogFile = Join-Path $logDir 'watchdog.log'
}

# 系统若配置过代理，会让本机探测被劫持（历史上出现过 HTTP_PROXY 劫持 curl 误判），这里一律直连
[System.Net.WebRequest]::DefaultWebProxy = $null

function Write-Log {
    param([string]$Level, [string]$Message)
    $line = "[{0}] {1,-5} {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    Write-Output $line
    # 简易滚动：超过 5MB 归档为 .1
    if ((Test-Path $LogFile) -and ((Get-Item $LogFile).Length -gt 5MB)) {
        Move-Item $LogFile "$LogFile.1" -Force
    }
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Test-BackendHealth {
    <# 返回 @{ Ok; Status; ElapsedMs; Detail } #>
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $resp = Invoke-WebRequest -Uri "$BaseUrl/api/health" -UseBasicParsing -TimeoutSec $TimeoutSeconds
        $sw.Stop()
        $json = $null
        try { $json = $resp.Content | ConvertFrom-Json } catch { }
        $status = if ($json -and $json.status) { $json.status } else { "HTTP$($resp.StatusCode)" }
        return [pscustomobject]@{
            Ok        = ($resp.StatusCode -eq 200 -and $status -eq 'UP')
            Status    = $status
            ElapsedMs = [int]$sw.ElapsedMilliseconds
            Detail    = "HTTP $($resp.StatusCode) db=$($json.db) uptime=$($json.uptimeSeconds)s"
        }
    } catch {
        $sw.Stop()
        return [pscustomobject]@{
            Ok        = $false
            Status    = 'UNREACHABLE'
            ElapsedMs = [int]$sw.ElapsedMilliseconds
            Detail    = $_.Exception.Message
        }
    }
}

function Test-TcpPort {
    param([int]$TargetPort)
    $c = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue
    return [bool]$c
}

function Test-HttpOk {
    param([string]$Url)
    try {
        $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSeconds
        return ($r.StatusCode -ge 200 -and $r.StatusCode -lt 400)
    } catch { return $false }
}

function Restart-Backend {
    param([string]$Reason)
    $holders = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($holders) {
        foreach ($h in ($holders | Select-Object -ExpandProperty OwningProcess -Unique)) {
            try {
                Write-Log 'WARN' "停止占用 $Port 的进程 PID=$h（原因：$Reason）"
                Stop-Process -Id $h -Force -ErrorAction SilentlyContinue
            } catch {
                Write-Log 'ERROR' "停止 PID=$h 失败：$($_.Exception.Message)"
            }
        }
        Start-Sleep -Seconds 3
    }
    try {
        $out = & powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'start-backend.ps1') -Port $Port 2>&1
        Write-Log 'INFO' "重启输出：$($out -join ' | ')"
    } catch {
        Write-Log 'ERROR' "重启失败：$($_.Exception.Message)"
        return $false
    }
    # 复检（给 JVM 一点时间完成 Spring 启动）
    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep -Seconds 2
        $h = Test-BackendHealth
        if ($h.Ok) {
            Write-Log 'INFO' "重启成功：$($h.Detail)（耗时 $($h.ElapsedMs)ms）"
            return $true
        }
    }
    Write-Log 'ERROR' '重启后健康探针仍不通过，请人工介入（查看 backend\app.log）'
    return $false
}

function Invoke-Preflight {
    Write-Output ''
    Write-Output '================ 演示前预检 ================'
    $rows = @()
    $rows += [pscustomobject]@{ 服务 = '前端 (3000)';       检查 = 'HTTP';   结果 = $(if (Test-HttpOk 'http://127.0.0.1:3000/') { 'PASS' } else { 'FAIL' }) }
    $h = Test-BackendHealth
    $rows += [pscustomobject]@{ 服务 = '后端健康 (/api/health)'; 检查 = 'HTTP+DB'; 结果 = $(if ($h.Ok) { 'PASS' } else { 'FAIL' }) }
    $rows += [pscustomobject]@{ 服务 = '后端响应耗时';      检查 = '< 1000ms'; 结果 = $(if ($h.ElapsedMs -lt 1000 -and $h.Ok) { 'PASS' } else { "FAIL ($($h.ElapsedMs)ms)" }) }
    $rows += [pscustomobject]@{ 服务 = 'Flink JobManager (18081)'; 检查 = 'HTTP'; 结果 = $(if (Test-HttpOk 'http://127.0.0.1:18081/jobs/overview') { 'PASS' } else { 'FAIL' }) }
    $rows += [pscustomobject]@{ 服务 = 'Flink SQL Gateway (18083)'; 检查 = 'HTTP'; 结果 = $(if (Test-HttpOk 'http://127.0.0.1:18083/v1/info') { 'PASS' } else { 'FAIL' }) }
    $rows += [pscustomobject]@{ 服务 = 'Kafka (29092)';     检查 = 'TCP';    结果 = $(if (Test-TcpPort 29092) { 'PASS' } else { 'FAIL' }) }
    $rows | Format-Table -AutoSize | Out-String | Write-Output
    $failed = @($rows | Where-Object { $_.结果 -ne 'PASS' }).Count
    Write-Log 'INFO' "预检完成：$($rows.Count - $failed)/$($rows.Count) 通过"
    Write-Output '============================================'
    exit $failed
}

# ---------------- 主流程 ----------------
if ($Preflight) { Invoke-Preflight }

if ($Once) {
    $h = Test-BackendHealth
    Write-Log $(if ($h.Ok) { 'INFO' } else { 'ERROR' }) "单次探针：$($h.Status)（$($h.ElapsedMs)ms）$($h.Detail)"
    exit $(if ($h.Ok) { 0 } else { 1 })
}

Write-Log 'INFO' "看门狗启动：$BaseUrl 每 ${IntervalSeconds}s 探测，连续 $FailureThreshold 次失败则重启（$LogFile）"
if ($NoRestart) { Write-Log 'WARN' '观察模式：只告警不重启' }

$consecutiveFailures = 0
$lastRestartAt = [DateTime]::MinValue
$restartTimes = New-Object System.Collections.ArrayList
$probeCount = 0

while ($true) {
    $probeCount++
    $h = Test-BackendHealth
    if ($h.Ok) {
        if ($consecutiveFailures -gt 0) { Write-Log 'INFO' "健康恢复（此前连续失败 $consecutiveFailures 次）" }
        $consecutiveFailures = 0
        Write-Log 'INFO' "健康 $($h.Status)（$($h.ElapsedMs)ms）$($h.Detail)"
    } else {
        $consecutiveFailures++
        Write-Log 'WARN' "探测失败（第 $consecutiveFailures/$FailureThreshold 次）：$($h.Status)（$($h.ElapsedMs)ms）$($h.Detail)"

        if ($consecutiveFailures -ge $FailureThreshold) {
            if ($NoRestart) {
                Write-Log 'ERROR' '判定不健康，但当前为观察模式，不执行重启'
                $consecutiveFailures = 0
            } else {
                $now = Get-Date
                $cooldownOk = (($now - $lastRestartAt).TotalSeconds -ge $RestartCooldownSeconds)
                # 清理 1 小时前的重启记录，判断频率上限
                $keep = @($restartTimes | Where-Object { ($now - $_).TotalHours -lt 1 })
                $restartTimes.Clear(); foreach ($k in $keep) { [void]$restartTimes.Add($k) }
                $rateOk = ($restartTimes.Count -lt $MaxRestartsPerHour)

                if (-not $cooldownOk) {
                    Write-Log 'WARN' "距上次重启不足 ${RestartCooldownSeconds}s，跳过本次重启"
                } elseif (-not $rateOk) {
                    Write-Log 'ERROR' "1 小时内已重启 $($restartTimes.Count) 次（上限 $MaxRestartsPerHour），停止自动重启，请人工排查"
                    exit 2
                } else {
                    Write-Log 'ERROR' "连续 $FailureThreshold 次探测失败，判定后端僵死，执行重启"
                    $lastRestartAt = $now
                    [void]$restartTimes.Add($now)
                    [void](Restart-Backend -Reason "健康探针连续 $FailureThreshold 次失败")
                    $consecutiveFailures = 0
                }
            }
        }
    }

    if ($MaxProbes -gt 0 -and $probeCount -ge $MaxProbes) {
        Write-Log 'INFO' "达到最大探测次数 $MaxProbes，退出（最后状态：$($h.Status)）"
        exit $(if ($h.Ok) { 0 } else { 1 })
    }
    Start-Sleep -Seconds $IntervalSeconds
}
