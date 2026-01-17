import ckan.plugins.toolkit as tk

CONF_LOGS_FOLDER = "ckanext.logs.logs_path"
CONF_LOGS_FILE_NAME = "ckanext.logs.log_filename"


def get_logs_folder() -> str | None:
    return tk.config[CONF_LOGS_FOLDER]
