import logging
from datetime import timezone

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)


class DataGovGrHarvester(object):
    '''
    Generic base class for data.gov.gr harvesters, providing a number of useful functions
    '''

    def __init__(self, name=None):
        # Don't call super() here as we're using multiple inheritance
        self.name = name

    def _fix_licenses(self, package_dict):
        '''
        Fix license information for the dataset.

        Args:
            package_dict (dict): The package dictionary to modify
        '''
        log.info("Fixing licenses for dataset: %s", package_dict.get('title'))
        # Implementation will be provided in subclasses
        pass

    def _preserve_resource_names(self, package_dict, harvest_object):
        '''
        Preserve resource names from the remote package.

        Args:
            package_dict (dict): The package dictionary to modify
            harvest_object: The harvest object containing the remote content
        '''
        log.info("Preserving resource names for dataset: %s", package_dict.get('title'))
        # Implementation will be provided in subclasses
        pass

    # Start hooks

    def modify_package_dict(self, package_dict, harvest_object):
        '''
        Allows custom harvesters to modify the package dict before
        creating or updating the actual package.
        
        This method can be overridden by subclasses to provide custom
        functionality.
        '''

        # Keep only harvesting-related metadata in extras
        harvesting_keys = {
            'harvest_object_id',
            'harvest_source_id', 
            'harvest_source_title',
            'harvest_source_url',
            'harvest_job_id',
            'guid',
            'source_hash'
        }
        
        if 'extras' in package_dict and isinstance(package_dict['extras'], list):
            filtered_extras = []
            
            for extra in package_dict['extras']:
                if isinstance(extra, dict) and 'key' in extra:
                    key = extra['key']
                    # Keep only harvesting-related keys
                    if key in harvesting_keys:
                        filtered_extras.append(extra)
                        
            package_dict['extras'] = filtered_extras
            log.info("Filtered extras to keep only harvesting metadata. Kept %d entries", len(filtered_extras))
        
        return package_dict

    # End hooks
