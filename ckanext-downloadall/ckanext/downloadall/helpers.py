def _get_pkg_value(pkg, key):
    """Safely get attribute or dict value from a package-like object."""
    if isinstance(pkg, dict):
        return pkg.get(key)
    return getattr(pkg, key, None)


def _get_dataset_type(pkg):
    """Return the dataset type if available, otherwise None."""
    for key in ('type', 'dataset_type', 'package_type'):
        value = _get_pkg_value(pkg, key)
        if value:
            return value
    return None


def is_data_service(pkg):
    """Return True when the given package is a data-service dataset."""
    dataset_type = _get_dataset_type(pkg)
    if dataset_type in ('data-service', 'data_service'):
        return True

    # Some scheming datasets store the type as an extra key/value pair
    extras = _get_pkg_value(pkg, 'extras')
    if isinstance(extras, dict):
        for key, value in extras.items():
            if key in ('dataset_type', 'package_type') and value in ('data-service', 'data_service'):
                return True
    elif isinstance(extras, list):
        for extra in extras:
            if isinstance(extra, dict):
                key = extra.get('key')
                value = extra.get('value')
            else:
                key = getattr(extra, 'key', None)
                value = getattr(extra, 'value', None)
            if key in ('dataset_type', 'package_type') and value in ('data-service', 'data_service'):
                return True
    return False


def pop_zip_resource(pkg):
    '''Finds the zip resource in a package's resources, removes it from the
    package and returns it. NB the package doesn't have the zip resource in it
    any more.
    '''
    zip_res = None
    non_zip_resources = []
    for res in pkg.get('resources', []):
        if res.get('downloadall_metadata_modified'):
            zip_res = res
        else:
            non_zip_resources.append(res)
    pkg['resources'] = non_zip_resources
    if is_data_service(pkg):
        return None
    return zip_res
