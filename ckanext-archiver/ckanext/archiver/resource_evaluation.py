NON_EVALUABLE = 'non_evaluable'
BROKEN = 'broken'

TABLEDESIGNER_REASON = 'Resource is a Table Designer resource'
NO_URL_REASON = 'Resource has no URL'
DOWNLOADALL_REASON = 'Resource is a Download All ZIP resource'


def _resource_field(resource, key, default=None):
    if isinstance(resource, dict):
        return resource.get(key, default)

    if hasattr(resource, key):
        return getattr(resource, key)

    extras = getattr(resource, 'extras', None)
    if extras:
        return extras.get(key, default)

    return default


def _normalized_url(resource):
    return (_resource_field(resource, 'url') or '').strip()


def get_non_evaluable_resource_reason(resource):
    if _resource_field(resource, 'downloadall_metadata_modified'):
        return DOWNLOADALL_REASON
    if _resource_field(resource, 'url_type') == 'tabledesigner':
        return TABLEDESIGNER_REASON
    if not _normalized_url(resource):
        return NO_URL_REASON
    return None


def get_resource_evaluation_state(resource, archival):
    if get_non_evaluable_resource_reason(resource):
        return NON_EVALUABLE
    if archival and archival.is_broken is True:
        return BROKEN
    return None
