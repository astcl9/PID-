[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][ValidateSet('baseline','agent','final-report')][string]$Mode,
  [Parameter(Mandatory=$true)][string]$TaskName,
  [Parameter(Mandatory=$true)][string]$Workspace,
  [string]$PromptFile = '',
  [string]$ThreadId = '',
  [string]$CodexExe = '',
  [int]$TestTimeoutMinutes = 20
)
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
$runRoot = Join-Path $Workspace '.tmp\overnight-2026-09-17'
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null
$stamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
$log = Join-Path $runRoot ("{0}-{1}.log" -f $TaskName, $stamp)

function Write-Log([string]$Text) {
  $line = "[{0}] {1}" -f (Get-Date).ToString('o'), $Text
  $line | Tee-Object -FilePath $log -Append
}

function Resolve-CodexExe([string]$Hint) {
  if ($Hint -and (Test-Path -LiteralPath $Hint)) { return $Hint }
  $binRoot = Join-Path $env:LOCALAPPDATA 'OpenAI\Codex\bin'
  if (Test-Path -LiteralPath $binRoot) {
    $found = Get-ChildItem -LiteralPath $binRoot -Filter 'codex.exe' -File -Recurse -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty FullName
    if ($found) { return $found }
  }
  return $null
}

function Get-Python([string]$Root) {
  $candidate = Join-Path $Root '.venv\Scripts\python.exe'
  if (Test-Path -LiteralPath $candidate) { return $candidate }
  return 'python'
}

function Test-Pytest([string]$Root, [string]$Tag) {
  $python = Get-Python $Root
  $report = Join-Path $runRoot ("{0}-pytest.txt" -f $Tag)
  Write-Log "PYTEST_START binary=$python report=$report"
  $job = Start-Job -ScriptBlock {
    param($py, $root, $target)
    Set-Location -LiteralPath $root
    & $py -m pytest -q *> $target
    exit $LASTEXITCODE
  } -ArgumentList $python, $Root, $report
  $finished = Wait-Job -Job $job -Timeout ($TestTimeoutMinutes * 60)
  if (-not $finished) {
    Stop-Job -Job $job -ErrorAction SilentlyContinue
    Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
    Write-Log 'PYTEST_RESULT=timeout'
    return $false
  }
  $jobState = $job.State
  Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
  $tail = ((Get-Content -LiteralPath $report -Tail 8 -ErrorAction SilentlyContinue) -join ' | ')
  Write-Log "PYTEST_STATE=$jobState TAIL=$tail"
  if ($tail -match '(\d+) failed') { Write-Log 'PYTEST_RESULT=failed'; return $false }
  if ($tail -match '(\d+) passed') { Write-Log 'PYTEST_RESULT=passed'; return $true }
  Write-Log 'PYTEST_RESULT=unknown'; return $false
}

function Save-DiffSnapshot([string]$Root, [string]$Tag) {
  $statusFile = Join-Path $runRoot ("{0}-git-status.txt" -f $Tag)
  $patchFile = Join-Path $runRoot ("{0}-changes.patch" -f $Tag)
  $statFile = Join-Path $runRoot ("{0}-diff-stat.txt" -f $Tag)
  (git -C $Root status --short 2>&1 | Out-String) | Set-Content -LiteralPath $statusFile -Encoding UTF8
  (git -C $Root diff --stat 2>&1 | Out-String) | Set-Content -LiteralPath $statFile -Encoding UTF8
  (git -C $Root diff 2>&1 | Out-String) | Set-Content -LiteralPath $patchFile -Encoding UTF8
  Write-Log "SNAPSHOT status=$statusFile stat=$statFile patch=$patchFile"
}

function Send-ThreadNotice([string]$CodexPath, [string]$Message, [string]$Target) {
  if (-not $Target) { Write-Log 'THREAD_NOTICE=skipped(no thread id)'; return }
  if (-not $CodexPath) { Write-Log 'THREAD_NOTICE=skipped(no codex exe)'; return }
  $attemptLog = Join-Path $runRoot 'thread-notice.log'
  for ($attempt = 1; $attempt -le 3; $attempt++) {
    $output = & $CodexPath queue --thread $Target --message $Message 2>&1
    $exitCode = $LASTEXITCODE
    $line = "[{0}] attempt={1} exit={2} {3}" -f (Get-Date).ToString('o'), $attempt, $exitCode, (($output | Out-String).Trim())
    $line | Add-Content -LiteralPath $attemptLog -Encoding UTF8
    if ($exitCode -eq 0) { Write-Log 'THREAD_NOTICE=ok'; return }
    Start-Sleep -Seconds 20
  }
  Write-Log 'THREAD_NOTICE=failed'
}

Write-Log "START task=$TaskName mode=$Mode host=$env:COMPUTERNAME user=$env:USERNAME"
Write-Log "WORKSPACE=$Workspace"
$codexExe = Resolve-CodexExe $CodexExe
Write-Log "CODEX_EXE=$codexExe"

Push-Location $Workspace
try {
  switch ($Mode) {

    'baseline' {
      $head = (git -C $Workspace rev-parse --short HEAD 2>&1 | Out-String).Trim()
      Write-Log "GIT_HEAD=$head"
      Save-DiffSnapshot $Workspace 'baseline'
      Test-Pytest $Workspace 'baseline' | Out-Null
      try {
        $health = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:8080/health' -TimeoutSec 10 -ErrorAction Stop
        Write-Log "HTTP_HEALTH=$($health.StatusCode)"
      } catch { Write-Log 'HTTP_HEALTH=unavailable' }
      $pg = docker ps --filter 'name=pid-postgres' --format '{{.Names}} {{.Status}}' 2>&1
      Write-Log "POSTGRES_CONTAINER=$((($pg | Out-String).Trim()))"
      $disk = Get-PSDrive -Name (Split-Path $Workspace -Qualifier).TrimEnd(':')
      Write-Log ("DISK_FREE_GB=" + [math]::Round($disk.Free / 1GB, 1))
      foreach ($bench in @('docs\benchmark-latest.json', 'docs\benchmark-postgres-latest.json')) {
        $path = Join-Path $Workspace $bench
        if (Test-Path -LiteralPath $path) { Write-Log "BENCH_PRESENT=$path" }
      }
    }

    'agent' {
      if (-not $PromptFile -or -not (Test-Path -LiteralPath $PromptFile)) {
        Write-Log "ERROR prompt file not found: $PromptFile"
        exit 2
      }
      if (-not $codexExe) {
        Write-Log 'ERROR codex.exe not found'
        exit 127
      }
      $prompt = Get-Content -LiteralPath $PromptFile -Raw -Encoding UTF8
      Write-Log ("PROMPT_FILE={0} PROMPT_CHARS={1}" -f $PromptFile, $prompt.Length)
      $final = Join-Path $runRoot ("{0}-{1}.final.md" -f $TaskName, $stamp)
      $agentLog = Join-Path $runRoot ("{0}-{1}.agent.txt" -f $TaskName, $stamp)
      $started = Get-Date
      # Prompt goes through stdin so long multi-line instructions are not limited
      # by the Windows command-line length.  Transient upstream capacity/502
      # errors are retried with backoff instead of losing the whole window.
      $agentExit = 1
      for ($try = 1; $try -le 3; $try++) {
        Write-Log "AGENT_ATTEMPT=$try"
        if ($try -gt 1) { Remove-Item -LiteralPath $final -Force -ErrorAction SilentlyContinue }
        $prompt | & $codexExe exec -C $Workspace -s workspace-write --color never -o $final 2>&1 |
          Tee-Object -FilePath $agentLog -Append | Out-Null
        $agentExit = $LASTEXITCODE
        $logText = Get-Content -LiteralPath $agentLog -Raw -ErrorAction SilentlyContinue
        if ($agentExit -eq 0) { break }
        if ($logText -match 'at capacity|502 Bad Gateway|Reconnecting') {
          Write-Log "AGENT_RETRYABLE_FAILURE exit=$agentExit"
          Start-Sleep -Seconds 90
          continue
        }
        Write-Log "AGENT_NON_RETRYABLE_FAILURE exit=$agentExit"
        break
      }
      $elapsed = [int]((Get-Date) - $started).TotalSeconds
      Write-Log "AGENT_EXIT=$agentExit AGENT_SECONDS=$elapsed FINAL=$final"
      if (Test-Path -LiteralPath $final) {
        $summary = Get-Content -LiteralPath $final -Raw -Encoding UTF8
        $preview = $summary.Substring(0, [Math]::Min(1500, $summary.Length))
        Write-Log "AGENT_SUMMARY_BEGIN`n$preview`nAGENT_SUMMARY_END"
      } else {
        Write-Log 'AGENT_SUMMARY=missing'
      }
      Test-Pytest $Workspace $TaskName | Out-Null
      Save-DiffSnapshot $Workspace $TaskName
    }

    'final-report' {
      $report = Join-Path $runRoot 'morning-report.md'
      $lines = New-Object System.Collections.Generic.List[string]
      $lines.Add('# 2026-09-18 早晨报告（夜间自动改进）')
      $lines.Add('')
      $lines.Add(('- 生成时间：{0}' -f (Get-Date).ToString('o')))
      $lines.Add(('- 工作区：{0}' -f $Workspace))
      $lines.Add(('- Git HEAD：{0}' -f ((git -C $Workspace rev-parse --short HEAD 2>&1 | Out-String).Trim())))
      $lines.Add('')
      $lines.Add('## 任务日志')
      Get-ChildItem -LiteralPath $runRoot -Filter '*.log' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime | ForEach-Object {
          $lines.Add(('- {0}（{1}）' -f $_.Name, $_.LastWriteTime.ToString('o')))
        }
      $lines.Add('')
      $lines.Add('## 各任务最终答复')
      Get-ChildItem -LiteralPath $runRoot -Filter '*.final.md' -File -ErrorAction SilentlyContinue |
        Sort-Object Name | ForEach-Object {
          $lines.Add(('### {0}' -f $_.Name))
          $lines.Add('')
          $lines.Add((Get-Content -LiteralPath $_.FullName -Raw -Encoding UTF8))
          $lines.Add('')
        }
      $lines.Add('## 测试结果')
      Get-ChildItem -LiteralPath $runRoot -Filter '*pytest.txt' -File -ErrorAction SilentlyContinue |
        Sort-Object Name | ForEach-Object {
          $tail = ((Get-Content -LiteralPath $_.FullName -Tail 3 -ErrorAction SilentlyContinue) -join ' ')
          $lines.Add(('- {0}: {1}' -f $_.Name, $tail))
        }
      $lines.Add('')
      $lines.Add('## 当前 Git 状态')
      $lines.Add('```text')
      $lines.Add(((git -C $Workspace status --short 2>&1 | Out-String).TrimEnd()))
      $lines.Add('```')
      $lines.Add('')
      $lines.Add('## 结论边界')
      $lines.Add('- 夜间任务只在本机运行，不代表已完成现场真实数据校准。')
      $lines.Add('- 夜间任务未执行 git commit / push，也未删除 data、postgres_data 或既有 .tmp 内容。')
      $lines.Add('- 逐任务差异补丁保存在本目录的 *.changes.patch，便于逐条审查。')
      $lines | Set-Content -LiteralPath $report -Encoding UTF8
      Write-Log "FINAL_REPORT=$report"
    }
  }
} catch {
  Write-Log ("EXCEPTION: " + ($_ | Out-String))
} finally {
  Pop-Location
}

Write-Log "END task=$TaskName mode=$Mode"

if ($Mode -eq 'final-report') {
  $reportPath = Join-Path $runRoot 'morning-report.md'
  if (Test-Path -LiteralPath $reportPath) {
    $notice = "夜间自动改进任务（2026-09-17 18:00 → 2026-09-18 08:00）已经全部结束。完整报告：$reportPath。请只回复一行『夜间任务已完成』，不要执行任何其他操作。"
    Send-ThreadNotice $codexExe $notice $ThreadId
  }
}