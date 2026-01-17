from __future__ import annotations

import gzip
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from ckanext.tables.shared import ListDataSource

from ckanext.logs import config

LOG_ENTRY_START_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+"
    r"(INFO|ERROR|WARNING|WARNI|DEBUG|CRITICAL)\s+"
    r"\[([^\]]+)\]\s*(.*)"
)

UWSGI_LOG_PATTERN = re.compile(
    r"""
    ^\[pid:                             # must start like a uwsgi log
    .*?                                 # skip until timestamp
    \[                                  # opening bracket before timestamp
    (?P<timestamp>                      # capture the timestamp
        [A-Za-z]{3}\s+                  # day of week, e.g. Sun
        [A-Za-z]{3}\s+                  # month, e.g. Dec
        \d{1,2}\s+                      # day of month
        \d{2}:\d{2}:\d{2}\s+            # time
        \d{4}                           # year
    )
    \]                                  # closing bracket
""",
    re.VERBOSE,
)


class LogDataSource(ListDataSource):
    """Data source for reading log files with multi-line entries."""

    def __init__(self, log_file: str, n_logs: int = 10000):
        self.logs_path = Path(config.get_logs_folder() or "")
        self.logs_filename = log_file
        self.n_logs = n_logs
        self.data: list[dict[str, Any]] = []
        self.filtered: list[dict[str, Any]] | None = None

        self._load_all_logs()

    def _load_all_logs(self):
        """Load all log entries from all files, newest first."""
        log_files = sorted(
            self.logs_path.glob(f"{self.logs_filename}*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        all_entries: list[dict[str, Any]] = []

        for log_file in log_files:
            entries = self._parse_lines(self._read_all_lines(log_file))
            all_entries.extend(entries)

            if len(all_entries) >= self.n_logs:
                break

        # sort by timestamp descending
        all_entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
        self.data = all_entries
        self.filtered = self.data

    def _read_all_lines(self, log_file: Path) -> list[str]:
        """Read all lines from a file (supports .gz)."""
        try:
            if log_file.suffix == ".gz":
                with gzip.open(log_file, "rt", errors="ignore") as f:
                    return f.readlines()
            else:
                with log_file.open("r", errors="ignore") as f:
                    return f.readlines()
        except Exception:  # noqa
            return []

    def _parse_lines(self, lines: list[str]) -> list[dict[str, Any]]:
        entries = []
        buffer = []  # buffer for multi-line messages
        current_entry = None

        for line in lines:
            if match := UWSGI_LOG_PATTERN.match(line):
                # Flush previous entry
                if current_entry:
                    if buffer:
                        current_entry["message"] += "\n" + "\n".join(buffer)
                    entries.append(current_entry)
                    buffer = []

                current_entry = {
                    "timestamp": self.parse_uwsgi_timestamp(match.group("timestamp")),
                    "level": "UWSGI",
                    "module": "uwsgi",
                    "message": line.strip(),
                }
            elif match := LOG_ENTRY_START_RE.match(line):
                # Flush previous entry
                if current_entry:
                    if buffer:
                        current_entry["message"] += "\n" + "\n".join(buffer)
                    entries.append(current_entry)
                    buffer = []

                timestamp, level, module, message = match.groups()
                current_entry = {
                    "timestamp": timestamp,
                    "level": level,
                    "module": module,
                    "message": message.strip(),
                }
            else:
                buffer.append(line.rstrip())

        # Flush last entry
        if current_entry:
            if buffer:
                current_entry["message"] += "\n" + "\n".join(buffer)
            entries.append(current_entry)

        return entries

    def parse_uwsgi_timestamp(self, timestamp: str) -> str | None:
        dt = datetime.strptime(timestamp, "%a %b %d %H:%M:%S %Y")  # noqa: DTZ007

        # to match the format of other log timestamps
        return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
