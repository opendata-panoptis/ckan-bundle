import logging
from ckan.plugins import toolkit
from ckan.lib.helpers import lang

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
