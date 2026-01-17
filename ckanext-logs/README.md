# ckanext-logs

A CKAN extension to view CKAN logs files in the CKAN web interface.

The extension provides a table view of log entries with pagination, sorting, and filtering capabilities.

You can navigate to a specific log table from the dashboard.

![alt text](./docs/dashboard.png)

Clicking on one of the log tables will open the log viewer:

![alt text](./docs/table.png)

## Requirements

Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.9 and earlier | no            |
| 2.10            | yes           |
| 2.11            | yes           |

## Installation

1. Install from source:

```
pip install -e .
```

2. Add `tables logs` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

## Config settings

```yaml
- key: ckanext.logs.logs_path
description: Specify the path to the logs folder
default: /var/log/ckan
```

## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
