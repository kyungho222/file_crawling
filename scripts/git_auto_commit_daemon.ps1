<#
  자동 커밋 데몬(백그라운드)
  - 20분마다 1회 자동 커밋 스크립트 실행
  - 동시 실행 방지(다른 데몬이 있으면 즉시 종료)
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
if (-not $repoRoot) { exit 0 }
Set-Location $repoRoot

$daemonLockPath = Join-Path $repoRoot ".git\\auto-commit-daemon.lock"
try {
  $daemonLockStream = [System.IO.File]::Open($daemonLockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
} catch {
  # 이미 데몬이 실행 중이면 종료
  exit 0
}

try {
  $onceScript = Join-Path $repoRoot "scripts\\git_auto_commit_once.ps1"
  if (-not (Test-Path $onceScript)) { exit 0 }

  while ($true) {
    try {
      powershell -NoProfile -ExecutionPolicy Bypass -File $onceScript | Out-Null
    } catch {
      # 무시하고 다음 주기로 진행
    }
    Start-Sleep -Seconds 1200  # 20분
  }
} finally {
  $daemonLockStream.Dispose()
}


