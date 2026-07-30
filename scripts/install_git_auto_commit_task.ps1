<#
  Windows 작업 스케줄러에 20분마다 자동 커밋 작업 등록
  - 현재 사용자로 실행(S4U), 비밀번호 입력 없이 등록
  - 이미 있으면 덮어씀
#>

$ErrorActionPreference = "Stop"

function Get-RepoRoot([string]$startDir) {
  $d = (Resolve-Path $startDir).Path
  while ($true) {
    if (Test-Path (Join-Path $d ".git")) { return $d }
    $parent = Split-Path $d -Parent
    if (-not $parent -or $parent -eq $d) { return $null }
    $d = $parent
  }
}

$repoRoot = Get-RepoRoot $PSScriptRoot
if (-not $repoRoot) {
  throw "repoRoot를 찾지 못했습니다. scripts 폴더가 Git repo 내부인지 확인하세요."
}

$taskName = "crawler_web_files08-auto-commit-20m"
$psExe = (Get-Command powershell.exe).Source
$scriptPath = Join-Path $repoRoot "scripts\\git_auto_commit_daemon.ps1"

if (-not (Test-Path $scriptPath)) {
  throw "스크립트를 찾지 못했습니다: $scriptPath"
}

$action = New-ScheduledTaskAction -Execute $psExe -Argument "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""

# 로그온 시 데몬 실행(데몬이 내부에서 20분마다 동작)
$trigger = New-ScheduledTaskTrigger -AtLogOn

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\\$env:USERNAME" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings

function Install-WithRegisterScheduledTask {
  Register-ScheduledTask -TaskName $taskName -InputObject $task -Force | Out-Null
}

function Install-WithSchTasks {
  # schtasks는 Task Scheduler의 권한 정책에 따라 Register-ScheduledTask보다 잘 되는 환경이 있어 fallback으로 사용
  $escapedScriptPath = $scriptPath.Replace('"', '""')
  $tr = "`"$psExe`" -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File `"$escapedScriptPath`""
  schtasks /Create /TN "$taskName" /TR "$tr" /SC ONLOGON /RL LIMITED /F | Out-Null
}

try {
  Install-WithRegisterScheduledTask
} catch {
  # 권한 문제(0x80070005 등)일 때 schtasks로 재시도
  try {
    Install-WithSchTasks
  } catch {
    Write-Host "설치 실패: $taskName"
    Write-Host "원인: 작업 스케줄러 등록 권한이 없습니다(정책/권한)."
    Write-Host "해결: PowerShell을 '관리자 권한으로 실행' 후 이 스크립트를 다시 실행하세요."
    throw
  }
}

Write-Host "설치 완료: $taskName"
Write-Host "로그인 시 데몬이 시작되며, 20분마다 변경사항이 있을 때만 '로컬 커밋'합니다(push는 하지 않음)."


