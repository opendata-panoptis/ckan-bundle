from ckan.common import config
from ckan.plugins import toolkit as tk
from ckan.common import _
import re


def archiver_resource_show(resource_id):
    data_dict = {'id': resource_id}
    return tk.get_action('archiver_resource_show')(data_dict)


def archiver_show_broken_status() -> bool:
    # Default: False αν λείπει από το ini
    return tk.asbool(config.get("ckanext.archiver.show_broken_status", False))


def archiver_is_resource_broken_html(resource):
    archival = resource.get('archiver')
    if not archival:
        return tk.literal('<!-- No archival info for this resource -->')
    extra_vars = {'resource': resource}
    extra_vars.update(archival)
    return tk.literal(
        tk.render('archiver/is_resource_broken.html',
                  extra_vars=extra_vars))


def archiver_show_cached_url() -> bool:
    # Default: False αν λείπει από το ini
    return tk.asbool(config.get("ckanext.archiver.show_cached_url", False))


def archiver_is_resource_cached_html(resource):
    archival = resource.get('archiver')
    if not archival:
        return tk.literal('<!-- No archival info for this resource -->')
    extra_vars = {'resource': resource}
    extra_vars.update(archival)
    return tk.literal(
        tk.render('archiver/is_resource_cached.html',
                  extra_vars=extra_vars))


# Replacement for the core ckan helper 'format_resource_items'
# but with our own blacklist
def archiver_format_resource_items(items):
    blacklist = ['archiver', 'qa']
    items_ = [item for item in items
              if item[0] not in blacklist]
    import ckan.lib.helpers as ckan_helpers
    return ckan_helpers.format_resource_items(items_)


def archiver_status_label(status):
    '''Return a localized label for an Archival.status value.'''
    if not status:
        return status
    if status == 'not recorded':
        return _('not recorded')
    mapping = {
        'Archived successfully': _('Archived successfully'),
        'Content has not changed': _('Content has not changed'),
        'URL invalid': _('URL invalid'),
        'URL request failed': _('URL request failed'),
        'Download error': _('Download error'),
        'Non-evaluable': _('Non-evaluable'),
        'Chose not to download': _('Chose not to download'),
        'Download failure': _('Download failure'),
        'System error during archival': _('System error during archival'),
    }
    return mapping.get(status, status)


def archiver_reason_label(reason):
    '''Return a localized version of an Archival.reason message where possible.'''
    if not reason:
        return reason
    if reason == 'not recorded':
        return _('not recorded')

    # Server status error: "Server reported status error: %s %s"
    m = re.match(r'^Server reported status error: (\d+)\s+(.*)$', reason)
    if m:
        code, text = m.groups()
        return _('Server reported status error: %s %s') % (code, text)

    # Content-length after streaming was %i
    m = re.match(r'^Content-length after streaming was (\d+)$', reason)
    if m:
        length = int(m.group(1))
        return _('Content-length after streaming was %i') % length

    # Content-length %s exceeds maximum allowed value %s
    m = re.match(r'^Content-length (\d+) exceeds maximum allowed value (\d+)$',
                 reason)
    if m:
        length, max_len = m.groups()
        return _('Content-length %s exceeds maximum allowed value %s') % (
            length, max_len)

    # Server content contained an API error message: %s
    m = re.match(r'^Server content contained an API error message: (.*)$',
                 reason)
    if m:
        snippet = m.group(1)
        return _('Server content contained an API error message: %s') % snippet

    # Connection / HTTP / timeout / redirects / generic download errors
    patterns = [
        (r'^Connection error: (.*)$', _('Connection error: %s')),
        (r'^Invalid HTTP response: (.*)$', _('Invalid HTTP response: %s')),
        (r'^Connection timed out after (\d+)s$', _('Connection timed out after %ss')),
        (r'^Error downloading: (.*)$', _('Error downloading: %s')),
        (r'^Error with the download: (.*)$', _('Error with the download: %s')),
        (r'^Error during request: (.*)$', _('Error during request: %s')),
        (r'^Error with the request: (.*)$', _('Error with the request: %s')),
    ]
    for pattern, tmpl in patterns:
        m = re.match(pattern, reason)
        if m:
            # All templates above have a single %s placeholder
            return tmpl % m.group(1)

    # Translate common substring from requests: "Max retries exceeded with url"
    if 'Max retries exceeded with url' in reason:
        reason = reason.replace(
            'Max retries exceeded with url',
            _('Max retries exceeded with url')
        )

    # Link checker messages
    static_mapping = {
        'Invalid URL or Redirect Link': _('Invalid URL or Redirect Link'),
        'Could not make HEAD request': _('Could not make HEAD request'),
        'Too many redirects': _('Too many redirects'),
        'Missing downloaded file information': _('Missing downloaded file information'),
        'Azure upload failed': _('Azure upload failed'),
        'Resource is a Download All ZIP resource': _(
            'Resource is a Download All ZIP resource'),
        'Resource is a Table Designer resource': _(
            'Resource is a Table Designer resource'),
        'Resource has no URL': _('Resource has no URL'),
        'No value for ckanext-archiver.cache_url_root in config': _(
            'No value for ckanext-archiver.cache_url_root in config'),
    }
    if reason in static_mapping:
        return static_mapping[reason]

    return reason
