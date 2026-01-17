from __future__ import annotations

import io
import os
import zipfile
from datetime import datetime, timezone
from typing import TypedDict

from flask import Blueprint, Response, send_file
from flask.views import MethodView

import ckan.plugins.toolkit as tk

from ckanext.tables.shared import GenericTableView

from ckanext.logs.config import get_logs_folder
from ckanext.logs.table import LogsTable

bp = Blueprint("logs", __name__, url_prefix="/ckan-admin/logs")


class LogFileInfo(TypedDict):
    name: str
    size: int
    mtime: datetime


def before_request() -> None:
    try:
        tk.check_access("sysadmin", {"user": tk.current_user.name})
    except tk.NotAuthorized:
        tk.abort(403, tk._("Need to be system administrator to administer"))


class LogsDashboardView(MethodView):
    _MAIN_LOG = "ckan-error.log"

    def get(self):
        logs_folder = get_logs_folder()
        all_log_files = self.get_logs_files(logs_folder)
        total_size = sum(f["size"] for f in all_log_files)

        # UI: show only the "latest" (main) ckan-error log
        logs_files = [f for f in all_log_files if f["name"] == self._MAIN_LOG]

        # Size shown in UI: total size of main + rotated together
        logs_sizes = self._calculate_logs_sizes(all_log_files, logs_files)

        return tk.render(
            "logs/dashboard.html",
            {
                "logs_folder": logs_folder,
                "logs_files": logs_files,
                "total_size": total_size,
                "logs_sizes": logs_sizes,
            },
        )

    def get_logs_files(self, logs_folder: str | None) -> list[LogFileInfo]:
        if not logs_folder or not os.path.exists(logs_folder):
            return []

        files = []

        for name in os.listdir(logs_folder):
            path = os.path.join(logs_folder, name)
            if os.path.isfile(path):
                size = os.path.getsize(path)
                mtime = os.path.getmtime(path)
                files.append(
                    LogFileInfo(
                        name=name,
                        size=size,
                        mtime=datetime.fromtimestamp(mtime, tz=timezone.utc),
                    )
                )

        return sorted(files, key=lambda x: x["mtime"], reverse=True)

    def _calculate_logs_sizes(self, all_files: list[LogFileInfo], log_files: list[LogFileInfo]) -> dict[str, int]:
        result: dict[str, int] = {}

        for log_file in log_files:
            # For "ckan-error.log" include "ckan-error.log" + "ckan-error.log.N"
            prefix = log_file["name"]
            total_size = 0

            for file in all_files:
                if file["name"] == prefix or file["name"].startswith(prefix + "."):
                    total_size += file["size"]

            result[log_file["name"]] = total_size

        return result


class LogsDownloadView(MethodView):
    _MAIN_LOG = "ckan-error.log"

    def get(self) -> Response:
        logs_folder = get_logs_folder()

        if not logs_folder or not os.path.exists(logs_folder):
            tk.abort(404, "Logs directory not found")

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in os.listdir(logs_folder):
                # Download All: restrict to ckan-error.log + rotations
                if name != self._MAIN_LOG and not name.startswith(self._MAIN_LOG + "."):
                    continue

                path = os.path.join(logs_folder, name)
                if os.path.isfile(path):
                    zf.write(path, arcname=name)

        zip_buffer.seek(0)

        return send_file(zip_buffer, as_attachment=True, download_name="logs_archive.zip", mimetype="application/zip")


class LogsExportTableView(MethodView):
    _MAIN_LOG = "ckan-error.log"

    def get(self, log_file: str) -> Response:
        logs_folder = get_logs_folder()

        if not logs_folder or not os.path.exists(logs_folder):
            tk.abort(404, "Logs directory not found")

        # Only allow exporting the main ckan-error.log "family"
        if log_file != self._MAIN_LOG:
            tk.abort(404, "Log file not found")

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in os.listdir(logs_folder):
                if name != self._MAIN_LOG and not name.startswith(self._MAIN_LOG + "."):
                    continue

                path = os.path.join(logs_folder, name)

                if os.path.isfile(path):
                    zf.write(path, arcname=name)

        zip_buffer.seek(0)

        return send_file(zip_buffer, as_attachment=True, download_name="logs_archive.zip", mimetype="application/zip")


class LogsGenericTableView(GenericTableView):
    def get(self, log_file: str) -> str | Response:   # type: ignore
        table = self.table(log_file=log_file)  # type: ignore

        if exporter_name := tk.request.args.get("exporter"):
            return self._export(table, exporter_name)

        if tk.request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return self._ajax_data(table)

        return table.render_table(
            breadcrumb_label=self.breadcrumb_label,
            page_title=self.page_title,
        )

    def post(self, log_file: str, **_kwargs: object) -> str | Response:
        return self.get(log_file)


bp.before_request(before_request)

bp.add_url_rule("/dashboard", view_func=LogsDashboardView.as_view("dashboard"))
bp.add_url_rule("/export", view_func=LogsDownloadView.as_view("download"))
bp.add_url_rule("/export_log_file/<log_file>", view_func=LogsExportTableView.as_view("export_log_file"))
bp.add_url_rule("/table/<log_file>", view_func=LogsGenericTableView.as_view("table", table=LogsTable))