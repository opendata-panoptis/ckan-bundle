from __future__ import annotations

import ckan.plugins.toolkit as tk
from ckan.types import Context

from ckanext.tables.formatters import TrimStringFormatter
from ckanext.tables.shared import ActionHandlerResult, ColumnDefinition, TableActionDefinition, TableDefinition

from ckanext.logs.data_source import LogDataSource
from ckanext.logs.formatters import LogsDialogModalFormatter


class LogsTable(TableDefinition):
    """Table definition for the logs dashboard."""

    def __init__(self, log_file: str):
        """Initialize the table definition."""
        self.log_file = log_file

        super().__init__(
            name="logs",
            data_source=LogDataSource(log_file=log_file),
            columns=[
                ColumnDefinition(field="timestamp", width=180, resizable=False),
                ColumnDefinition(field="level", width=100, resizable=False),
                ColumnDefinition(field="module", width=200),
                ColumnDefinition(
                    field="message",
                    formatters=[(TrimStringFormatter, {"max_length": 88})],
                    tabulator_formatter="html",
                ),
                ColumnDefinition(
                    title=" ",
                    field="details",
                    formatters=[(LogsDialogModalFormatter, {"max_length": 88})],
                    tabulator_formatter="html",
                    width=50,
                    sortable=False,
                    resizable=False,
                    filterable=False,
                ),
            ],
            table_actions=[
                TableActionDefinition(
                    action="export_logs",
                    label=tk._("Export Logs"),
                    icon="fa fa-download",
                    callback=self.table_action_export_logs,
                ),
            ]
        )

    def table_action_export_logs(self) -> ActionHandlerResult:
        return ActionHandlerResult(
            success=True,
            error=None,
            redirect=tk.h.url_for("logs.export_log_file", log_file=self.log_file),
        )

    @classmethod
    def check_access(cls, context: Context) -> None:
        tk.check_access("sysadmin", context)
