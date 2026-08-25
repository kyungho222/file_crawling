from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "dashboard" / "file_crawl_stage_lab.html"


def main() -> None:
    html = PAGE.read_text(encoding="utf-8")
    assert "let refreshInFlight = false" in html
    assert "let refreshQueued = false" in html
    assert "function scheduleRefresh" in html
    assert "if (refreshInFlight)" in html
    assert "eventSource.addEventListener('stage', () => scheduleRefresh())" in html
    assert "eventSource.onerror = () => scheduleRefresh(250)" in html
    assert 'id="toggle-monitoring"' in html
    assert "function stopRealtimeMonitoring" in html
    assert "function startRealtimeMonitoring" in html
    assert "sessionStorage.setItem(MONITORING_KEY, \"off\")" in html
    assert "if (!realtimeMonitoringEnabled) return;" in html
    assert "sessionStorage.getItem(MONITORING_KEY) === 'on'" in html


if __name__ == "__main__":
    main()
    print("file crawl stage lab realtime refresh guard ok")
