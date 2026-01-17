from typing import Any

import ckan.plugins as p
import ckan.plugins.toolkit as tk
from ckan import types
from ckan.common import CKANConfig

from ckanext.logs.table import LogsTable


@tk.blanket.config_declarations
@tk.blanket.blueprints
@tk.blanket.helpers
class LogsPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer)
    p.implements(p.ISignal)

    # IConfigurer

    def update_config(self, config_: CKANConfig) -> None:
        tk.add_template_directory(config_, "templates")

    # # ISignal

    def get_signal_subscriptions(self) -> types.SignalMapping:
        return {
            tk.signals.ckanext.signal("ckanext.tables.register_tables"): [
                self.collect_tables
            ],
        }

    def collect_tables(self, _: None) -> dict[str, type[Any]]:
        return {"logs": LogsTable}
