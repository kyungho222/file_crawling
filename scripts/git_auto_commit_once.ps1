<#
  20분마다 실행되는 "1회용" 자동 커밋 스크립트.
  - 변경사항이 있을 때만 add/commit
  - merge/rebase 중이면 안전하게 스킵
  - 동시 실행 방지를 위해 .git 내부 lock 사용
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

if (-not (Test-Path ".git")) { exit 0 }

# 동시 실행 방지(다른 인스턴스가 lock을 잡고 있으면 종료)
$lockPath = Join-Path $repoRoot ".git\\auto-commit.lock"
try {
  $lockStream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
} catch {
  exit 0
}

try {
  # Git 작업 중(merge/rebase 등)에는 자동 커밋하지 않음
  if (Test-Path ".git\\MERGE_HEAD") { exit 0 }
  if (Test-Path ".git\\rebase-apply") { exit 0 }
  if (Test-Path ".git\\rebase-merge") { exit 0 }
  if (Test-Path ".git\\CHERRY_PICK_HEAD") { exit 0 }

  $changes = git status --porcelain
  if ($LASTEXITCODE -ne 0) { exit 0 }

  if (-not $changes) { exit 0 }

  git add -A
  if ($LASTEXITCODE -ne 0) { exit 0 }

  # 스테이징된 변경이 없으면 커밋하지 않음
  git diff --cached --quiet
  if ($LASTEXITCODE -eq 0) { exit 0 }

  $msg = "auto: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
  git commit -m $msg | Out-Null
  exit 0
} finally {
  $lockStream.Dispose()
}


