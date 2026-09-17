[CmdletBinding()]
param(
  [string]$Workspace = (Get-Location).Path,
  [string]$ThreadId = '01a0a467-88eb-7c33-a970-a2d4fb957b88',
  [string]$TaskPrefix = 'PID-Overnight-20260917',
  [switch]$RemoveOnly
)
$ErrorActionPreference = 'Continue'
$runner = Join-Path $Workspace 'scripts\overnight-2026-09-17\Invoke-OvernightTask.ps1'
$promptsDir = Join-Path $Workspace 'scripts\overnight-2026-09-17\prompts'
$runnerPath = "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"

function New-TaskTrigger([string]$Time) {
  New-ScheduledTaskTrigger -Once -At $Time
}

$agentTasks = @(
  @{ Slot='02-Boundaries';        At='2026-09-17T18:00:00'; Prompt='01-boundaries.md';        Timeout=105 },
  @{ Slot='03-Segmentation';      At='2026-09-17T19:30:00'; Prompt='02-segmentation.md';      Timeout=105 },
  @{ Slot='04-ApiTests';          At='2026-09-17T21:00:00'; Prompt='03-tests.md';             Timeout=105 },
  @{ Slot='05-PostgresBenchmark'; At='2026-09-17T23:00:00'; Prompt='04-postgres-benchmark.md'; Timeout=105 },
  @{ Slot='06-Frontend';          At='2026-09-18T01:00:00'; Prompt='05-frontend.md';          Timeout=105 },
  @{ Slot='07-OfflineAssets';     At='2026-09-18T02:30:00'; Prompt='06-offline-assets.md';    Timeout=105 },
  @{ Slot='08-ManualOverride';    At='2026-09-18T04:00:00'; Prompt='07-manual-override.md';   Timeout=105 },
  @{ Slot='09-DockerHardening';   At='2026-09-18T05:30:00'; Prompt='08-docker-hardening.md';  Timeout=105 },
  @{ Slot='10-RegressionDocs';    At='2026-09-18T07:00:00'; Prompt='09-regression-docs.md';   Timeout=45 }
)
$auxTasks = @(
  @{ Slot='01-Baseline';          At='2026-09-17T17:55:00'; Mode='baseline'; Timeout=15 },
  @{ Slot='11-FinalReport';       At='2026-09-18T08:00:00'; Mode='final-report'; Timeout=30 }
)

foreach ($item in @($agentTasks + $auxTasks)) {
  $name = "{0}-{1}" -f $TaskPrefix, $item.Slot
  Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
}
if ($RemoveOnly) {
  Write-Output "Removed scheduled tasks matching prefix $TaskPrefix."
  return
}

$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 0) -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

foreach ($item in @($agentTasks + $auxTasks)) {
  $name = "{0}-{1}" -f $TaskPrefix, $item.Slot
  $trigger = New-TaskTrigger $item.At
  if ($item.Mode -eq 'baseline' -or $item.Mode -eq 'final-report') {
    $args = "-NoProfile -ExecutionPolicy Bypass -File `"$runner`" -Mode $($item.Mode) -TaskName `"$name`" -Workspace `"$Workspace`" -ThreadId `"$ThreadId`""
  } else {
    $promptPath = Join-Path $promptsDir $item.Prompt
    if (-not (Test-Path -LiteralPath $promptPath)) { Write-Output "MISSING prompt: $promptPath"; continue }
    $args = "-NoProfile -ExecutionPolicy Bypass -File `"$runner`" -Mode agent -TaskName `"$name`" -Workspace `"$Workspace`" -PromptFile `"$promptPath`" -ThreadId `"$ThreadId`" -TestTimeoutMinutes $($item.Timeout)"
  }
  $action = New-ScheduledTaskAction -Execute $runnerPath -Argument $args
  try {
    Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force -ErrorAction Stop | Out-Null
    Write-Output "REGISTERED $name @ $($item.At)"
  } catch {
    Write-Output "FAILED $name : $($_.Exception.Message)"
  }
}
Write-Output 'Done.'