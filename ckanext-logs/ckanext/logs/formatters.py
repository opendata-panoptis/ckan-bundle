import ckan.plugins.toolkit as tk

from ckanext.tables import types
from ckanext.tables.formatters import BaseFormatter, DialogModalFormatter


class LogsDialogModalFormatter(BaseFormatter):
    """Formatter to show log details in a modal dialog."""

    def format(self, value: types.Value, options: types.Options) -> types.FormatterResult:
        formatter = DialogModalFormatter(self.column, self.row, self.initial_row, self.table)
        value = self.initial_row["message"]

        options.update({"template": "logs/formatters/dialog_modal.html"})

        return formatter.format(value, options)
