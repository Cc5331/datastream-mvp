# 字段拼接测试数据 — 端到端实跑脚本
# ================================================================
# 把 test-resources/dags 下的字段拼接 DAG 真实提交到后端 → 等作业跑完 →
# 把落盘输出与 test-resources/data/concat_expected_*.csv 逐行比对，输出差异报告。
#
# 前置：后端 8080 与 Flink 已启动（仓库根 start-all.bat）。
#
# 用法：
#   pwsh scripts/run-field-concat-demo.ps1                          # 默认 02-csv-concat-csv.json
#   pwsh scripts/run-field-concat-demo.ps1 -Dag 02b-csv-concat-null-guard.json
#   pwsh scripts/run-field-concat-demo.ps1 -Username admin -Password '你的密码'
#
# 说明：DAG 里的 csv_output 指向 concat_expected_*.csv 本身，用来做「期望 vs 实际」对照；
#       脚本会先把期望文件复制到 output/ 下再提交，绝不覆盖期望值文件。
param(
    [string]$Dag = '02-csv-concat-csv.json',
    [string]$BaseUrl = 'http://localhost:18080',
    [string]$Username = 'admin',
    [string]$Password = $(if ($env:ADMIN_PASSWORD) { $env:ADMIN_PASSWORD } else { 'admin123' }),
    [int]$TimeoutSec = 300
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
$DagPath = Join-Path $Root "test-resources\dags\$Dag"

if (-not (Test-Path $DagPath)) { throw "DAG 不存在: $DagPath" }
if ([string]::IsNullOrEmpty($Password)) {
    $secure = Read-Host "请输入 $Username 的登录密码" -AsSecureString
    $Password = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
}

function Api {
    param([string]$Method, [string]$Path, $Body, [string]$Token)
    $headers = @{}
    if ($Token) { $headers['Authorization'] = "Bearer $Token" }
    $reqArgs = @{ Method = $Method; Uri = "$BaseUrl$Path"; Headers = $headers; TimeoutSec = 30 }
    if ($null -ne $Body) {
        $reqArgs['Body'] = ([Text.Encoding]::UTF8.GetBytes(($Body | ConvertTo-Json -Depth 30)))
        $reqArgs['ContentType'] = 'application/json; charset=utf-8'
    }
    Invoke-RestMethod @reqArgs
}

Write-Host "== 字段拼接实跑：$Dag ==" -ForegroundColor Cyan

# 1) 登录
$login = Api POST '/api/auth/login' @{ username = $Username; password = $Password }
$token = $login.token
if (-not $token) { throw '登录失败：未拿到 token' }
Write-Host "  [1/5] 登录成功：$($login.user.displayName)"

# 2) 准备对照：期望文件 → output/ 下的实跑目标路径
$dagObj = Get-Content $DagPath -Raw -Encoding UTF8 | ConvertFrom-Json
$expectedNode = $dagObj.nodes | Where-Object { $_.type -eq 'csv_output' } | Select-Object -First 1
$expectedRel = $expectedNode.params.path                                  # test-resources/data/concat_expected_*.csv
$expectedPath = Join-Path $Root ($expectedRel -replace '/', '\')
if (-not (Test-Path $expectedPath)) { throw "期望输出文件不存在: $expectedPath" }

$runDir = Join-Path $Root 'output'
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$runPath = Join-Path $runDir (Split-Path $expectedPath -Leaf)
if (Test-Path $runPath) { Remove-Item $runPath -Force }

$expectedNode.params.path = $runPath
$dagJsonObj = @{ jobName = "$($dagObj.jobName) [实跑]"; parallelism = $dagObj.parallelism; nodes = $dagObj.nodes; edges = $dagObj.edges }

# 3) 建作业并提交
$job = Api POST '/api/jobs' @{
    name    = $dagJsonObj.jobName
    dagJson = ($dagJsonObj | ConvertTo-Json -Depth 30 -Compress)
}
Write-Host "  [2/5] 作业已创建：#$($job.id)"

$submitted = Api POST "/api/jobs/$($job.id)/submit"
Write-Host "  [3/5] 已提交，状态：$($submitted.status)  期望输出 → $runPath"

# 4) 轮询
$deadline = (Get-Date).AddSeconds($TimeoutSec)
$status = $submitted.status
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 3
    $cur = Api GET "/api/jobs/$($job.id)"
    if ($cur.status -ne $status) { Write-Host "       状态 → $($cur.status)"; $status = $cur.status }
    if ($status -in @('COMPLETED', 'FAILED', 'CANCELLED')) { break }
}
if ($status -ne 'COMPLETED') {
    Write-Host "  [4/5] 作业未成功：$status" -ForegroundColor Red
    Api GET "/api/jobs/$($job.id)/logs" | Select-Object -Last 15 | ForEach-Object { "        $_" }
    exit 2
}
Write-Host "  [4/5] 作业 COMPLETED"

# 5) 对照
if (-not (Test-Path $runPath)) { Write-Host "  [5/5] 输出文件未生成：$runPath" -ForegroundColor Red; exit 3 }
$exp = Import-Csv $expectedPath -Encoding UTF8
$act = Import-Csv $runPath -Encoding UTF8
$expCols = ($exp[0].PSObject.Properties.Name) -join ','
$actCols = ($act[0].PSObject.Properties.Name) -join ','

$problems = @()
if ($expCols -ne $actCols) { $problems += "表头不一致`n  期望: $expCols`n  实际: $actCols" }
if ($exp.Count -ne $act.Count) { $problems += "行数不一致：期望 $($exp.Count) / 实际 $($act.Count)" }

$limit = [Math]::Min($exp.Count, $act.Count)
for ($i = 0; $i -lt $limit; $i++) {
    foreach ($col in $exp[0].PSObject.Properties.Name) {
        if ($exp[$i].$col -ne $act[$i].$col) {
            $problems += "第 $($i + 1) 行 列[$col]：期望 '$($exp[$i].$col)' / 实际 '$($act[$i].$col)'"
            break
        }
    }
    if ($problems.Count -ge 12) { $problems += '（差异过多，已截断）'; break }
}

if ($problems.Count -eq 0) {
    Write-Host "  [5/5] ✅ 逐行逐列一致：$($exp.Count) 行 × $($exp[0].PSObject.Properties.Count) 列" -ForegroundColor Green
    Write-Host "        期望: $expectedPath"
    Write-Host "        实际: $runPath"
    exit 0
}
Write-Host "  [5/5] ❌ 存在差异：" -ForegroundColor Red
$problems | ForEach-Object { Write-Host "        $_" }
exit 1
