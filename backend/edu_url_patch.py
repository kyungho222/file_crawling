import inspect
import logging
import textwrap


logger = logging.getLogger("backend.edu_url_patch")
_PATCHED = False


def _fix_cancel_block(source: str) -> str:
    lines = source.splitlines()
    out: list[str] = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]
        stripped = line.lstrip()

        # If we see the cancel-if, replace the whole fragment with a canonical, well-indented block
        if stripped.startswith('if status == "cancel":') or stripped.startswith("if status == 'cancel':"):
            base_indent = len(line) - len(stripped)

            # search forward for the end marker
            j = i + 1
            end_found = False
            while j < n:
                if lines[j].lstrip().startswith("url_result = await completed_task"):
                    end_found = True
                    break
                j += 1

            # if no end marker, just copy the line and continue
            if not end_found:
                out.append(line)
                i += 1
                continue

            # build canonical replacement lines, indented relative to the if line
            indent_spaces = " " * (base_indent + 4)
            canonical = [
                line,
                indent_spaces + 'logger.info(f"[작업 취소] job_id: {job_id}")',
                indent_spaces + "for task in tasks:",
                indent_spaces + "    if not task.done():",
                indent_spaces + "        task.cancel()",
                "",
                indent_spaces + "await asyncio.gather(*tasks, return_exceptions=True)",
                indent_spaces + 'logger.info(f"[작업 취소 완료] 모든 URL 처리 태스크 정리됨: job_id={job_id}")',
                "",
            ]

            out.extend(canonical)
            # append the original end-marker line (so original flow continues)
            out.append(lines[j])
            i = j + 1
            continue

        # otherwise copy unchanged
        out.append(line)
        i += 1

    return "\n".join(out)


def _patch_crawl_and_process_url_parallel() -> bool:
    import edu.url_edu as current

    try:
        source = inspect.getsource(current.crawl_and_process_url_parallel)
    except OSError as exc:
        logger.warning("Failed to read crawl_and_process_url_parallel source: %s", exc)
        return False

    fixed_source = _fix_cancel_block(source)
    if fixed_source == source:
        return False

    exec(textwrap.dedent(fixed_source), current.__dict__)
    return True


def _patch_should_check_url_changes() -> None:
    import edu.url_edu as current

    async def should_check_url_changes_safe(
        url: str, table_name: str, dbname: str, force_check: bool = False
    ) -> bool:
        try:
            if force_check:
                return True

            from db.db_config import connect_db, return_connection  # type: ignore
            import json
            from datetime import datetime, timedelta

            conn = await connect_db(dbname)

            query_jsonb = """
                SELECT content_metadata
                FROM {table_name}
                WHERE content = $1
                AND content_metadata IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
            """.format(table_name=table_name)

            result_jsonb = await conn.fetchrow(query_jsonb, url)

            last_check = None
            update_frequency = "1_day"

            if result_jsonb and result_jsonb.get("content_metadata"):
                content_metadata_raw = result_jsonb["content_metadata"]

                if isinstance(content_metadata_raw, str):
                    try:
                        content_metadata = json.loads(content_metadata_raw)
                    except json.JSONDecodeError:
                        content_metadata = {}
                else:
                    content_metadata = content_metadata_raw

                if isinstance(content_metadata, dict):
                    last_check_str = content_metadata.get("last_check")
                    update_frequency = content_metadata.get("update_frequency", "1_day")

                    if not last_check_str:
                        return True

                    try:
                        if last_check_str.endswith("Z"):
                            last_check_str = last_check_str.replace("Z", "+00:00")
                        last_check = datetime.fromisoformat(last_check_str)
                    except (ValueError, AttributeError):
                        return True
            else:
                return True

            if not last_check:
                return True

            frequency_hours = {
                "1_hour": 1,
                "1_day": 24,
                "1_week": 168,
                "1_month": 720,
            }

            check_interval_hours = frequency_hours.get(update_frequency or "1_day", 24)
            next_check_time = last_check + timedelta(hours=check_interval_hours)

            return datetime.now() >= next_check_time

        except Exception as exc:
            logger.error("should_check_url_changes failed: %s", exc)
            return True
        finally:
            if "conn" in locals():
                await return_connection(conn, dbname)

    should_check_url_changes_safe.__doc__ = getattr(
        current.should_check_url_changes, "__doc__", None
    )
    current.should_check_url_changes = should_check_url_changes_safe


def apply_patch() -> None:
    global _PATCHED
    if _PATCHED:
        return

    try:
        _patch_crawl_and_process_url_parallel()
        _patch_should_check_url_changes()
        _PATCHED = True
    except Exception as exc:
        logger.exception("edu.url_edu patch failed: %s", exc)


apply_patch()
