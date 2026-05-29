import logging
from ckan.plugins import toolkit
from ckan.lib.helpers import lang
from markupsafe import Markup, escape

log = logging.getLogger(__name__)

def get_municipality_name():
    """Returns the municipality name based on the current language."""
    try:
        current_lang = lang()
        log.debug(f"get_municipality_name called with lang: {current_lang}")

        if current_lang == "el":
            return "Δήμος Αθηναίων"
        return "City of Athens"

    except Exception as e:
        log.error(f"Error in get_municipality_name: {str(e)}")
        return "Δήμος Αθηναίων"  # Default to Greek

def get_featured_datasets(limit=3):
    """Returns a list of featured datasets."""
    try:
        # Get datasets tagged as 'featured'
        datasets = toolkit.get_action('package_search')(
            data_dict={
                'fq': 'featured:true',
                'rows': limit,
                'sort': 'metadata_modified desc'
            }
        )
        return datasets.get('results', [])
    except Exception as e:
        log.error(f"Error in get_featured_datasets: {str(e)}")
        return []

def get_featured_categories():
    """Returns a list of featured groups/categories."""
    try:
        # Get featured groups from CKAN
        groups = toolkit.get_action('group_list')(
            data_dict={'all_fields': True, 'sort': 'package_count desc', 'limit': 3}
        )
        return groups
    except Exception as e:
        log.error(f"Error in get_featured_categories: {str(e)}")
        return []

def get_recent_datasets(limit=3):
    """Returns a list of most recently modified datasets."""
    try:
        # Get recent datasets from CKAN
        datasets = toolkit.get_action('package_search')(
            data_dict={
                'rows': limit,
                'sort': 'metadata_modified desc'
            }
        )
        return datasets.get('results', [])
    except Exception as e:
        log.error(f"Error in get_recent_datasets: {str(e)}")
        return []

def get_site_statistics():
    """Returns a dict with site statistics."""
    try:
        # Get stats from CKAN
        stats = {}
        stats['dataset_count'] = toolkit.get_action('package_search')({}, {'rows': 0})['count']
        stats['group_count'] = len(toolkit.get_action('group_list')({}, {}))
        stats['organization_count'] = len(toolkit.get_action('organization_list')({}, {}))
        stats['resource_count'] = toolkit.get_action('resource_search')({}, {'query': {'limit': 0}})['count']
        return stats
    except Exception as e:
        log.error(f"Error in get_site_statistics: {str(e)}")
        return {
            'dataset_count': 0,
            'group_count': 0,
            'organization_count': 0,
            'resource_count': 0
        }


def _render_athens_nodes(parts, nodes, longnames, base_url, use_longnames,
                         is_top=False):
    css_class = 'org-tree org-tree--top' if is_top else 'org-tree org-tree--nested'
    parts.append(u'<ul class="{}">'.format(css_class))

    for node in nodes:
        name = node['name']
        title = node['title']
        node_id = node['id']
        highlighted = node.get('highlighted', False)

        if use_longnames:
            longname = longnames.get(node_id, '')
            if longname:
                display_text = u'{} ({})'.format(longname, title)
            else:
                display_text = title
        else:
            display_text = title

        active_class = u' is-active' if highlighted else u''
        parts.append(u'<li class="org-tree-item{}" id="node_{}">'.format(
            active_class, escape(name)))

        url = base_url.replace('__placeholder__', name)
        link = u'<a href="{}">{}</a>'.format(escape(url), escape(display_text))

        if node.get('children'):
            open_attr = u' open' if highlighted else u''
            parts.append(
                u'<details class="org-tree-branch"{}>'.format(open_attr))
            parts.append(u'<summary class="org-tree-row org-tree-summary">')
            parts.append(u'<span class="org-tree-toggle" aria-hidden="true">'
                         u'</span>')
            parts.append(u'<span class="org-tree-label">{}</span>'.format(link))
            parts.append(u'</summary>')
            _render_athens_nodes(parts, node['children'], longnames, base_url,
                                 use_longnames)
            parts.append(u'</details>')
        else:
            parts.append(u'<div class="org-tree-row org-tree-leaf">')
            parts.append(link)
            parts.append(u'</div>')

        parts.append(u'</li>')

    parts.append(u'</ul>')


def render_athens_tree_html(top_nodes, use_longnames=False):
    from ckanext.hierarchy.helpers import _collect_node_ids, _bulk_get_longnames

    longnames = {}
    if use_longnames:
        all_ids = _collect_node_ids(top_nodes)
        longnames = _bulk_get_longnames(all_ids)

    base_url = toolkit.url_for('organization.read', id='__placeholder__')

    parts = [u'<nav class="org-tree-wrap" aria-label="Φορείς">']
    _render_athens_nodes(parts, top_nodes, longnames, base_url,
                         use_longnames, is_top=True)
    parts.append(u'</nav>')

    return Markup(u''.join(parts))
