<#
  Windows 작업 스케줄러의 자동 커밋 작업 제거
#>

$ErrorActionPreference = "Stop"

$taskName = "crawler_web_files08-auto-commit-20m"

try {
  try {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
  } catch {
    # schtasks로 만들어진 작업/권한 정책 환경에서도 삭제되도록 fallback
    schtasks /Delete /TN "$taskName" /F | Out-Null
  }
  Write-Host "삭제 완료: $taskName"
} catch {
  Write-Host "작업이 없거나 삭제 실패: $taskName"
}


