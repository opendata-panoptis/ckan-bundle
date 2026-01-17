import math


def logs_printable_file_size(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0 bytes"

    size_name = ("bytes", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(float(size_bytes) / p, 1)

    return f"{s} {size_name[i]}"

def logs_header_icon_enabled() -> bool:
    """
    Config key: ckanext.logs.header_icon.enabled
    Default: False (if not set, icon is hidden)
    """
    from ckan.plugins import toolkit as tk

    raw = tk.config.get("ckanext.logs.header_icon.enabled", False)

    if isinstance(raw, bool):
        return raw

    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")