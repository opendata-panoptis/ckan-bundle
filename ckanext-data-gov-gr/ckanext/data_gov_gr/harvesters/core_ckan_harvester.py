import json
import logging
import datetime
from datetime import timezone
from urllib.parse import urlparse

from ckan import model
from ckan.plugins import toolkit
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
import ckan.plugins as plugins

from ckanext.data_gov_gr.harvesters.base import DataGovGrHarvester
from ckanext.data_gov_gr import helpers as data_gov_helpers

log = logging.getLogger(__name__)


# Define constants for tag validation
MIN_TAG_LENGTH = 2
MAX_TAG_LENGTH = 100


class CoreCkanHarvester(DataGovGrHarvester, CKANHarvester):
    '''
    Custom CKAN Harvester for core CKAN datasets
    '''
    _licence_vocabulary_codes_cache = None

    def __init__(self, name=None):
        super(CoreCkanHarvester, self).__init__(name)
        log.info("Core CKAN Harvester initialized")

    def info(self):
        return {
            'name': 'core_ckan_harvester',
            'title': 'Core CKAN Harvester',
            'description': 'Harvester for core CKAN datasets with custom name-to-tag mapping',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.info("Gather stage started for job: %s", harvest_job.id)
        result = super(CoreCkanHarvester, self).gather_stage(harvest_job)
        log.debug("Gather stage returned %d objects", len(result) if result else 0)
        return result

    def fetch_stage(self, harvest_object):
        log.info("Fetch stage started for object: %s", harvest_object.id)
        result = super(CoreCkanHarvester, self).fetch_stage(harvest_object)
        log.debug("Fetch stage completed with result: %s", result)
        return result

    def import_stage(self, harvest_object):
        log.info("Import stage started for object: %s", harvest_object.id)
        result = super(CoreCkanHarvester, self).import_stage(harvest_object)

        # Post-import isopen override - after all CKAN processing
        if result and hasattr(result, 'data') and result.data:
            try:
                # Parse remote data again to check isopen
                remote_package_dict = json.loads(harvest_object.content)
                if remote_package_dict.get('isopen') is True:
                    result.data['isopen'] = True
                    log.info("🔥🔥 FINAL isopen override in import_stage - forcing True")
            except Exception as e:
                log.error(f"Error in post-import isopen override: {e}")

        log.debug("Import stage completed with result: %s", result)
        return result

    def modify_package_dict(self, package_dict, harvest_object):
        '''
        Modify the package dict to map the "name" field to tags and fix common mime types

        Args:
            package_dict (dict): The package dictionary to modify
            harvest_object: The harvest object containing metadata

        Returns:
            dict: Modified package dictionary
        '''
        log.debug("Starting package modifications for: %s", package_dict.get('id', 'unknown'))

        # Force update by setting metadata_modified to now
        package_dict['metadata_modified'] = datetime.datetime.now(timezone.utc).isoformat()

        try:
            # Parse once at the beginning
            try:
                remote_package_dict = json.loads(harvest_object.content)
            except Exception as e:
                log.error(f"Failed to parse harvest object content: {e}")
                remote_package_dict = {}

            # Debug: Log remote package content to check for isopen
            log.debug(f"Remote package keys: {list(remote_package_dict.keys())}")
            log.debug(f"Remote isopen value: {remote_package_dict.get('isopen')}")

            # Call the parent method first to filter extras
            package_dict = super(CoreCkanHarvester, self).modify_package_dict(
                package_dict, harvest_object
            )

            # Debug: Check if parent method kept isopen
            log.debug(f"Package dict after parent method: {package_dict.get('isopen')}")

            # Get isopen from remote package if available
            remote_isopen = remote_package_dict.get('isopen')
            log.debug(f"Getting isopen from remote: {remote_isopen}")
            if remote_isopen is not None:
                package_dict['isopen'] = remote_isopen
                log.debug(f"✅ Set isopen from remote package: {remote_isopen}")
            else:
                log.debug("❌ No isopen found in remote package")

            # Apply all fixes
            self._fix_required_fields(package_dict)
            self._fix_common_mime_types(package_dict, remote_package_dict)
            self._preserve_resource_names(package_dict, remote_package_dict)
            self._add_groups_as_tags(package_dict, remote_package_dict)
            self._fix_organization_mapping(package_dict, remote_package_dict)
            self._add_license_to_resources(package_dict)
            self._clean_package_tags(package_dict)
            self._add_harvest_metadata(package_dict, harvest_object)
            # Ensure access_rights is always set to PUBLIC for CKAN-harvested datasets
            self._set_default_access_rights_public(package_dict)
            # Ensure applicable_legislation is set for PUBLIC datasets
            self._ensure_applicable_legislation(package_dict)
            
            # Remove original description when we have translated version
            if 'notes_translated-el' in package_dict:
                package_dict.pop('notes', None)

        except Exception as e:
            log.error(
                f"Error modifying package dict for dataset {package_dict.get('id', 'unknown')}: {e}",
                exc_info=True
            )
            # Continue with partially modified package_dict rather than failing

        # Fix isopen by ensuring license is properly set as open
        try:
            remote_isopen = remote_package_dict.get('isopen')
            license_id = package_dict.get('license_id')
            log.info(f"License processing - remote isopen: {remote_isopen}, license_id: {license_id}")
            
            if remote_isopen is True and license_id:
                # Map various license formats to their correct open equivalents in CKAN
                license_mapping = {
                    # Creative Commons variations (all versions)
                    'CC-BY-4.0': 'cc-by', 'CC-BY-3.0': 'cc-by', 'CC-BY-2.5': 'cc-by', 'CC-BY-2.0': 'cc-by', 'CC-BY': 'cc-by',
                    'CC-BY-SA-4.0': 'cc-by-sa', 'CC-BY-SA-3.0': 'cc-by-sa', 'CC-BY-SA-2.5': 'cc-by-sa', 'CC-BY-SA': 'cc-by-sa',
                    'CC0': 'cc-zero', 'CC-0': 'cc-zero', 'CC0-1.0': 'cc-zero', 'CCZERO': 'cc-zero',
                    # Open Data Commons variations (all formats)
                    'ODC-BY': 'odc-by', 'ODC-ODbL': 'odc-odbl', 'ODC-PDDL': 'odc-pddl',
                    'ODBL': 'odc-odbl', 'PDDL': 'odc-pddl', 'ODC-DBY': 'odc-by',
                    'OPEN-DATA-COMMONS-BY': 'odc-by', 'OPEN-DATA-COMMONS-ODBL': 'odc-odbl', 'OPEN-DATA-COMMONS-PDDL': 'odc-pddl',
                    # GNU variations
                    'GFDL': 'gfdl', 'GNU-FDL': 'gfdl', 'GNU-FDL-1.3': 'gfdl',
                    'GNU-FREE-DOCUMENTATION-LICENSE': 'gfdl',
                    # Government and public sector licenses
                    'UK-OGL': 'uk-ogl', 'OGL': 'uk-ogl', 'OPEN-GOVERNMENT': 'uk-ogl',
                    'OPEN-GOVERNMENT-LICENCE': 'uk-ogl', 'UK-OPEN-GOVERNMENT-LICENCE': 'uk-ogl',
                    # Generic open licenses
                    'OTHER-OPEN': 'other-open', 'OTHER-PD': 'other-pd', 'OTHER-AT': 'other-at',
                    'OPEN': 'other-open', 'PUBLIC-DOMAIN': 'other-pd', 'ATTRIBUTION': 'other-at',
                    # European Union specific licenses
                    'EU-ODBL': 'odc-odbl', 'EU-PDDL': 'odc-pddl', 'EU-BY': 'odc-by',
                    # International variations
                    'CREATIVE-COMMONS-ATTRIBUTION': 'cc-by', 'CREATIVE-COMMONS-ATTRIBUTION-SHARE-ALIKE': 'cc-by-sa',
                    'CREATIVE-COMMONS-ZERO': 'cc-zero',
                    # Non-Commercial variations (mapping to closed but tracking)
                    'CC-BY-NC': 'cc-nc', 'CC-BY-NC-SA': 'cc-nc-sa', 'CREATIVE-COMMONS-NON-COMMERCIAL': 'cc-nc',
                    # Other open license variations
                    'OPENDATA-COMMONS-ATTRIBUTION': 'odc-by',
                    'OPENDATA-COMMONS-OPEN-DATABASE-LICENSE': 'odc-odbl',
                    'OPENDATA-COMMONS-PUBLIC-DOMAIN-DEDICATION': 'odc-pddl',
                }
                
                # Try to find a mapped license (case insensitive)
                mapped_license_id = None
                try:
                    for mapped_id, ckan_id in license_mapping.items():
                        if license_id.upper() == mapped_id.upper():
                            mapped_license_id = ckan_id
                            break

                    # If no mapping found, try to use original license_id
                    if not mapped_license_id:
                        mapped_license_id = license_id
                        log.info(f"Using original license_id: {license_id} (no mapping found)")

                    if mapped_license_id != license_id:
                        package_dict['license_id'] = mapped_license_id
                        log.info(f"🔥 MAPPED license {license_id} -> {mapped_license_id}")
                except Exception as e:
                    log.warning(f"Error mapping license {license_id}, using original: {e}")
                    mapped_license_id = license_id
                
                # Ensure the license is marked as open in the license register
                try:
                    from ckan.model.license import LicenseRegister
                    licenses = LicenseRegister()
                    license_obj = licenses.get(mapped_license_id)
                except Exception as e:
                    log.warning(f"Error accessing license register: {e}")
                    license_obj = None
                
                if license_obj:
                    # Check if license is already marked as open
                    current_isopen = license_obj.isopen() if hasattr(license_obj, 'isopen') else False
                    log.info(f"License {mapped_license_id} is currently marked as open: {current_isopen}")
                    
                    # If license is not marked as open, force it to be open
                    if not current_isopen:
                        # Check if this should be an open license based on common patterns
                        license_title = getattr(license_obj, 'title', '').lower()
                        license_id_lower = mapped_license_id.lower()
                        
                        is_likely_open = (
                            # CKAN open license IDs
                            'cc-by' in license_id_lower or
                            'cc-by-sa' in license_id_lower or
                            'cc-zero' in license_id_lower or
                            'odc-by' in license_id_lower or
                            'odc-odbl' in license_id_lower or
                            'odc-pddl' in license_id_lower or
                            'gfdl' in license_id_lower or
                            'uk-ogl' in license_id_lower or
                            'other-open' in license_id_lower or
                            'other-pd' in license_id_lower or
                            'other-at' in license_id_lower or
                            # Generic CC patterns
                            'cc-' in license_id_lower or
                            'creative commons' in license_title.lower() or
                            # Open Data Commons patterns  
                            'opendata commons' in license_title.lower() or
                            'open data commons' in license_title.lower() or
                            # Public domain patterns
                            'public domain' in license_title.lower() or
                            'domaine public' in license_title.lower() or
                            # Government patterns
                            'open government' in license_title.lower() or
                            'government licence' in license_title.lower() or
                            'government license' in license_title.lower() or
                            'licence ouverte' in license_title.lower() or
                            # GNU patterns
                            'gnu free documentation' in license_title.lower() or
                            'gnu fdl' in license_title.lower() or
                            # Generic open patterns
                            'open licence' in license_title.lower() or
                            'open license' in license_title.lower() or
                            'libre' in license_title.lower() or
                            # European patterns
                            'eu-odbl' in license_id_lower or
                            'eu-pddl' in license_id_lower or
                            'eu-by' in license_id_lower or
                            # Zero patterns
                            'cc0' in license_id_lower or
                            'zero' in license_title.lower()
                        )
                        
                        if is_likely_open:
                            if hasattr(license_obj, 'od_conformance'):
                                license_obj.od_conformance = 'approved'
                            if hasattr(license_obj, 'osd_conformance'):
                                license_obj.osd_conformance = 'approved'
                            log.info(f"🔥 MARKED license {mapped_license_id} as OPEN by setting conformance to 'approved'")
                        else:
                            log.warning(f"License {mapped_license_id} doesn't appear to be an open license")
                    else:
                        log.info(f"License {mapped_license_id} is already properly marked as open")
                else:
                    log.warning(f"License {mapped_license_id} not found in license register")
                    
                    # Try to use a generic open license as fallback
                    try:
                        fallback_licenses = [
                            # Creative Commons (most common)
                            'cc-by', 'cc-by-sa', 'cc-zero',
                            # Open Data Commons
                            'odc-by', 'odc-odbl', 'odc-pddl',
                            # Generic and other
                            'other-open', 'other-pd', 'other-at',
                            # GNU and Government
                            'gfdl', 'uk-ogl'
                        ]
                        for fallback_id in fallback_licenses:
                            fallback_obj = licenses.get(fallback_id)
                            if fallback_obj and fallback_obj.isopen():
                                package_dict['license_id'] = fallback_id
                                log.info(f"🔥 FALLBACK: Using {fallback_id} instead of unknown license {license_id}")
                                break
                        else:
                            log.warning(f"No suitable open license fallback found for {license_id}, keeping original")
                    except Exception as e:
                        log.warning(f"Error finding license fallback: {e}, keeping original license")
            else:
                log.info(f"Not processing license - remote isopen: {remote_isopen}, license_id: {license_id}")
                
        except NameError:
            # remote_package_dict not defined due to earlier parsing error
            log.error("❌ remote_package_dict not available for license processing")
        except Exception as e:
            log.error(f"❌ Error in license processing: {e}", exc_info=True)
        return package_dict

    def _fix_organization_mapping(self, package_dict, remote_package_dict):
        '''Maps the organization from the remote package to the publisher field'''
        try:
            remote_org = remote_package_dict.get('organization')

            if not remote_org:
                return

            publisher = {
                'name': remote_org.get('title') or remote_org.get('name'),
                'identifier': remote_org.get('id'),
            }

            remote_description = remote_org.get('description')
            if isinstance(remote_description, str) and remote_description.strip():
                publisher['description'] = remote_description.strip()

            remote_uri = remote_org.get('uri')
            remote_url = remote_org.get('url')

            extras = remote_org.get('extras') or []
            if not isinstance(extras, list):
                extras = []

            for extra in extras:
                if not isinstance(extra, dict):
                    continue
                key = (extra.get('key') or '').strip().lower()
                value = extra.get('value')
                if not isinstance(value, str) or not value.strip():
                    continue

                if not remote_uri and key in {'uri', 'publisher_uri'}:
                    remote_uri = value
                if not remote_url and key in {'url', 'website', 'homepage', 'publisher_url'}:
                    remote_url = value

                if remote_uri and remote_url:
                    break

            if isinstance(remote_uri, str) and remote_uri.strip():
                publisher['uri'] = remote_uri.strip()

            if isinstance(remote_url, str) and remote_url.strip():
                publisher['url'] = remote_url.strip()

            # Remove empty values
            publisher = {k: v for k, v in publisher.items() if v}

            if publisher:
                package_dict['publisher'] = [publisher]
                log.debug(f"Set publisher to: {publisher.get('name')}")

        except Exception as e:
            log.error(f"Error fixing organization mapping: {e}", exc_info=True)

    def _preserve_resource_names(self, package_dict, remote_package_dict):
        '''
        Preserve resource names and descriptions from the remote package by creating
        the `name_translated` and `description_translated` fields
        '''
        try:
            remote_resources = remote_package_dict.get('resources', [])
            local_resources = package_dict.get('resources', [])

            for remote_resource, local_resource in zip(remote_resources, local_resources):
                # Handle resource name from remote 'name' field
                remote_name = remote_resource.get('name')
                if remote_name and isinstance(remote_name, str):
                    # Create translated name field
                    local_resource['name_translated'] = {'el': remote_name.strip()}
                    # Remove original name field when we have translated version
                    local_resource.pop('name', None)
                    log.debug(f"Set name_translated for resource: {remote_name}")

                # Handle resource description
                remote_description = remote_resource.get('description')
                if remote_description and isinstance(remote_description, str):
                    # Create translated description field
                    local_resource['description_translated'] = {'el': remote_description.strip()}
                    # Remove original description field when we have translated version
                    local_resource.pop('description', None)
                    log.debug(f"Set description_translated for resource")

                # Handle resource title as fallback for name
                remote_title = remote_resource.get('title')
                if remote_title and isinstance(remote_title, str) and 'name_translated' not in local_resource:
                    # Use title as name if name wasn't provided
                    local_resource['name_translated'] = {'el': remote_title.strip()}
                    log.debug(f"Used title as name_translated for resource: {remote_title}")

        except Exception as e:
            log.error(f"Error preserving resource names: {e}", exc_info=True)

    def _ensure_translated_field(self, package_dict, field_name, default_value):
        """Helper to ensure translated fields exist to avoid repetition"""
        translated_key = f'{field_name}_translated-el'

        if translated_key not in package_dict:
            value = None
            if package_dict.get(field_name):
                value = package_dict[field_name]
            elif package_dict.get(f'{field_name}_translated-en'):
                value = package_dict[f'{field_name}_translated-en']
            elif default_value is not None:
                value = default_value
            if not isinstance(value, str):
                value = ''
            package_dict[translated_key] = value
            log.debug(f"Set {translated_key}: {package_dict[translated_key]}")

    def _fix_required_fields(self, package_dict):
        # Ensure title_translated-el exists
        self._ensure_translated_field(package_dict, 'title', 'Untitled Dataset')

        # Ensure notes_translated-el exists
        self._ensure_translated_field(package_dict, 'notes', 'Χωρίς περιγραφή')

    def _set_default_access_rights_public(self, package_dict):
        """Force access_rights to PUBLIC for harvested datasets.

        Uses the Publications Office authority URI for PUBLIC access right.
        Also removes any access_rights occurrences from extras to avoid conflicts.
        """
        try:
            public_uri = 'http://publications.europa.eu/resource/authority/access-right/PUBLIC'
            package_dict['access_rights'] = public_uri

            # Clean up potential duplicates in extras
            extras = package_dict.get('extras')
            if isinstance(extras, list):
                package_dict['extras'] = [
                    e for e in extras
                    if not (isinstance(e, dict) and (e.get('key') or '').strip().lower() == 'access_rights')
                ]
            log.debug("Set access_rights to PUBLIC for harvested dataset")
        except Exception as e:
            log.error(f"Error setting access_rights to PUBLIC: {e}")

    def _ensure_applicable_legislation(self, package_dict):
        """
        Ensure that the dataset has an ``applicable_legislation`` field set
        when access_rights is PUBLIC.

        Uses the same configuration keys as the manual UI:
        - ``ckanext.data_gov_gr.dataset.legislation.open`` for open datasets.
        """
        try:
            if not isinstance(package_dict, dict):
                return

            # Do not override an explicit value if it already exists
            existing = package_dict.get('applicable_legislation')
            if existing:
                return

            access_rights = package_dict.get('access_rights')
            if not isinstance(access_rights, str):
                return

            lowered = access_rights.strip().lower()
            if not (lowered.endswith('/public') or 'access-right/public' in lowered):
                return

            value = data_gov_helpers.get_config_value(
                'ckanext.data_gov_gr.dataset.legislation.open', ''
            )
            if not isinstance(value, str):
                return

            value = value.strip()
            if not value:
                return

            package_dict['applicable_legislation'] = [value]
        except Exception as e:
            log.error(f"Error ensuring applicable_legislation for CKAN dataset: {e}")

    def _fix_common_mime_types(self, package_dict, remote_package_dict):
        '''Convert mime types to IANA URIs by adding the IANA prefix'''
        mime_type_corrections = {
            'txt/csv': 'text/csv',  # Fix common typo
        }

        for i, resource in enumerate(package_dict.get('resources', [])):
            if not isinstance(resource, dict):
                continue

            mimetype = resource.get('mimetype')
            if not mimetype:
                continue

            try:
                # Skip if already in IANA format
                if mimetype.startswith('https://www.iana.org/assignments/media-types/'):
                    continue

                # Apply corrections
                mimetype = mime_type_corrections.get(mimetype, mimetype)

                # Convert to IANA URI format if it looks like a standard MIME type
                if '/' in mimetype and not mimetype.startswith('http'):
                    iana_uri = f'https://www.iana.org/assignments/media-types/{mimetype}'
                    resource['mimetype'] = iana_uri
                    log.debug(f"Converted mimetype for resource {i}: {mimetype} -> {iana_uri}")
            except Exception as e:
                log.warning(f"Failed to process mimetype for resource {i}, clearing mimetype and continuing: {e}")
                # Set mimetype to None (empty) but continue processing
                resource['mimetype'] = None
                continue

    def _add_groups_as_tags(self, package_dict, remote_package_dict):
        '''Adds the titles of the groups from the remote package as tags'''
        try:
            remote_groups = remote_package_dict.get('groups', [])

            if not remote_groups:
                return

            if 'tags' not in package_dict:
                package_dict['tags'] = []

            existing_tag_names = {tag.get('name') for tag in package_dict['tags']}

            for group in remote_groups:
                try:
                    group_title = group.get('title', '').strip()
                    if group_title and group_title not in existing_tag_names:
                        package_dict['tags'].append({'name': group_title})
                        log.debug(f"Added group title as tag: {group_title}")
                except Exception as e:
                    log.warning(f"Error adding group as tag, skipping: {e}")
                    continue

        except Exception as e:
            log.error(f"Error adding groups as tags: {e}", exc_info=True)

    def _is_valid_tag_name(self, tag_name):
        '''
        Basic validation for tag names based on CKAN requirements

        Args:
            tag_name (str): The tag name to validate

        Returns:
            bool: True if valid, False otherwise
        '''
        if not tag_name or not isinstance(tag_name, str):
            return False

        tag_name = tag_name.strip()

        # Basic CKAN tag validation rules
        if len(tag_name) < MIN_TAG_LENGTH or len(tag_name) > MAX_TAG_LENGTH:
            return False

        # Check for invalid characters
        invalid_chars = set('",\'')
        if any(char in tag_name for char in invalid_chars):
            return False

        return True

    def _clean_package_tags(self, package_dict):
        '''
        Clean all tags in the package_dict to ensure they are valid for CKAN
        
        Args:
            package_dict (dict): The package dictionary to modify
        '''
        try:
            if 'tags' not in package_dict or not package_dict['tags']:
                log.debug("No tags to clean")
                return
            
            cleaned_tags = []
            for tag in package_dict['tags']:
                try:
                    if isinstance(tag, dict) and 'name' in tag:
                        original_name = tag['name']
                        cleaned_name = self._clean_single_tag(original_name)

                        # Only add tag if cleaning succeeded and result is not empty
                        if cleaned_name:
                            cleaned_tags.append({'name': cleaned_name})
                            if original_name != cleaned_name:
                                log.info(f"Cleaned tag: '{original_name}' -> '{cleaned_name}'")
                        else:
                            log.warning(f"Removed invalid tag: '{original_name}'")
                    else:
                        log.warning(f"Invalid tag format: {tag}")
                except Exception as e:
                    log.warning(f"Error cleaning individual tag, skipping: {e}")
                    continue
            
            package_dict['tags'] = cleaned_tags
            log.info(f"Tags cleaned: {len(cleaned_tags)} valid tags remaining")
            
        except Exception as e:
            log.error(f"Error cleaning tags: {e}", exc_info=True)

    def _clean_single_tag(self, tag_name):
        '''
        Clean a single tag name by removing invalid characters and handling special cases

        Args:
            tag_name (str): The tag name to clean

        Returns:
            str: Cleaned tag name with only valid characters
        '''
        if not tag_name or not isinstance(tag_name, str):
            return ''
        
        # Replace common problematic characters with valid alternatives
        cleaned = tag_name.replace(':', '-')  # Replace colons with hyphens
        cleaned = cleaned.replace('+', '-plus')  # Replace plus with -plus
        cleaned = cleaned.replace('&', 'and')  # Replace ampersand with 'and'
        cleaned = cleaned.replace('\'', '')  # Remove apostrophes
        cleaned = cleaned.replace('"', '')  # Remove quotes
        cleaned = cleaned.replace('«', '')  # Remove Greek opening quotes
        cleaned = cleaned.replace('»', '')  # Remove Greek closing quotes
        cleaned = cleaned.replace('(', '')  # Remove opening parentheses
        cleaned = cleaned.replace(')', '')  # Remove closing parentheses
        
        # Keep only alphanumeric characters, spaces, hyphens, underscores, and dots
        cleaned = ''.join(char for char in cleaned if char.isalnum() or char in ' -_.')
        
        # Remove multiple consecutive spaces and trim
        cleaned = ' '.join(cleaned.split())
        
        # Truncate to maximum allowed length (100 characters for CKAN tags)
        if len(cleaned) > MAX_TAG_LENGTH:
            original_length = len(cleaned)
            cleaned = cleaned[:MAX_TAG_LENGTH]  # Truncate to exact length
            log.warning(f"Tag truncated from {original_length} to {MAX_TAG_LENGTH} characters: '{cleaned}'")
        
        return cleaned.strip()

    def _add_harvest_metadata(self, package_dict, harvest_object):
        '''
        Add harvest metadata to the package dict to ensure proper tracking
        
        Args:
            package_dict (dict): The package dictionary to modify
            harvest_object: The harvest object containing metadata
        '''
        try:
            # Ensure extras list exists
            if 'extras' not in package_dict:
                package_dict['extras'] = []
            
            # Check if harvest metadata already exists
            existing_keys = {extra.get('key') for extra in package_dict['extras']}
            
            # Add harvest object ID
            if 'harvest_object_id' not in existing_keys and harvest_object.id:
                package_dict['extras'].append({
                    'key': 'harvest_object_id',
                    'value': harvest_object.id
                })
                log.debug(f"Added harvest_object_id: {harvest_object.id}")
            
            # Add harvest source ID from the harvest source configuration
            harvest_source_id = harvest_object.source.id
            if 'harvest_source_id' not in existing_keys and harvest_source_id:
                package_dict['extras'].append({
                    'key': 'harvest_source_id',
                    'value': harvest_source_id
                })
                log.debug(f"Added harvest_source_id: {harvest_source_id}")
            
            # Add harvest source title
            harvest_source_title = harvest_object.source.title
            if 'harvest_source_title' not in existing_keys and harvest_source_title:
                package_dict['extras'].append({
                    'key': 'harvest_source_title',
                    'value': harvest_source_title
                })
                log.debug(f"Added harvest_source_title: {harvest_source_title}")
            
            # Add harvest source URL
            harvest_source_url = harvest_object.source.url
            if 'harvest_source_url' not in existing_keys and harvest_source_url:
                package_dict['extras'].append({
                    'key': 'harvest_source_url',
                    'value': harvest_source_url
                })
                log.debug(f"Added harvest_source_url: {harvest_source_url}")
            
            log.info(f"Added harvest metadata: {len([e for e in package_dict['extras'] if e.get('key') in ['harvest_object_id', 'harvest_source_id']])} harvest entries")
            
        except Exception as e:
            log.error(f"Error adding harvest metadata: {e}", exc_info=True)


    def _add_license_to_resources(self, package_dict):
        '''
        Add dataset license information to each resource

        Args:
            package_dict (dict): The package dictionary to modify
        '''
        license_id = package_dict.get('license_id')
        license_title = package_dict.get('license_title')
        license_url = package_dict.get('license_url')
        
        # Get is_open from package
        is_open = package_dict.get('isopen')

        # Skip if no license information available
        if not any([license_id, license_title, license_url, is_open is not None]):
            log.debug("No license information found in package")
            return

        resources = package_dict.get('resources', [])
        if not resources:
            log.debug("No resources found in package")
            return

        # Add license information to each resource
        for i, resource in enumerate(resources):
            if not isinstance(resource, dict):
                continue
                
            try:
                # Add license fields to resource
                if license_id:
                    resource['license_id'] = license_id
                if license_title:
                    resource['license_title'] = license_title
                if license_url:
                    resource['license_url'] = license_url

                mapped_license_value = self._map_license_to_eu_uri(license_url, license_id)
                if mapped_license_value:
                    resource['license'] = mapped_license_value
                elif license_url:
                    log.debug(f"Resource {i}: license URL '{license_url}' could not be mapped to EU authority URI")

                if is_open is not None:
                    resource['is_open'] = is_open

                log.debug(f"Added license info to resource {i}: {license_id or license_title} (open: {is_open}, mapped: {mapped_license_value})")
            except Exception as e:
                log.warning(f"Failed to process license for resource {i}, skipping license processing: {e}")
                # Continue processing other resources even if license processing fails for this one
                continue

    def _map_license_to_eu_uri(self, license_url, license_id):
        """
        Map a given license URL or ID to the Publications Office authority URI
        expected by the Licence vocabulary.
        """
        code = None

        if license_url and isinstance(license_url, str):
            code = self._extract_license_code_from_url(license_url)

        if not code and license_id and isinstance(license_id, str):
            code = self._normalize_license_id_to_code(license_id)

        if not code:
            return None

        code = code.upper()
        valid_codes = self._get_valid_licence_codes()
        if valid_codes and code not in valid_codes:
            log.debug(f"License code '{code}' not present in Licence vocabulary, skipping mapping")
            return None

        return f'http://publications.europa.eu/resource/authority/licence/{code}'

    def _get_valid_licence_codes(self):
        """
        Load and cache the set of valid licence codes from the Licence vocabulary.
        """
        if CoreCkanHarvester._licence_vocabulary_codes_cache is not None:
            return CoreCkanHarvester._licence_vocabulary_codes_cache

        try:
            vocabulary = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                {}, {'id': 'Licence'}
            )
            tags = vocabulary.get('tags', [])
            codes = set()
            for tag in tags:
                value_uri = tag.get('value_uri')
                if value_uri and isinstance(value_uri, str):
                    code = value_uri.rstrip('/').split('/')[-1].upper()
                    codes.add(code)
                elif tag.get('name'):
                    code = tag['name'].rstrip('/').split('/')[-1].upper()
                    codes.add(code)

            CoreCkanHarvester._licence_vocabulary_codes_cache = codes
            log.debug(f"Loaded {len(codes)} licence codes from vocabulary")
            return codes
        except Exception as e:
            log.warning(f"Could not load Licence vocabulary codes: {e}")
            CoreCkanHarvester._licence_vocabulary_codes_cache = set()
            return CoreCkanHarvester._licence_vocabulary_codes_cache

    def _extract_license_code_from_url(self, license_url):
        """
        Attempt to derive the EU Publications licence code from a given URL.
        """
        try:
            url = license_url.strip()
            if not url:
                return None

            lower_url = url.lower()
            if 'publications.europa.eu/resource/authority/licence/' in lower_url:
                return url.rstrip('/').split('/')[-1]

            parsed = urlparse(url if '://' in url else f'https://{url}')
            netloc = parsed.netloc.lower()
            segments = [seg for seg in parsed.path.split('/') if seg]

            if 'creativecommons.org' in netloc and segments:
                return self._map_creative_commons_segments(segments)

            if ('opendatacommons.org' in netloc or 'opendefinition.org' in netloc) and len(segments) >= 2:
                licence_key = segments[1].lower()
                odc_mapping = {
                    'pddl': 'ODC_PDDL',
                    'odbl': 'ODC_BL',
                    'by': 'ODC_BY',
                    'by-sa': 'ODC_BY',
                    'by-odbl': 'ODC_BL'
                }
                if licence_key in odc_mapping:
                    return odc_mapping[licence_key]

            if 'gnu.org' in netloc and segments:
                if 'fdl' in segments[-1].lower():
                    return 'GFDL_1_3'
                if 'gpl' in segments[-1].lower():
                    return 'GPL_3_0'

            if 'opensource.org' in netloc and segments:
                last = segments[-1].lower()
                opensource_mapping = {
                    'mit': 'MIT',
                    'apache-2.0': 'APACHE_2_0',
                    'apache-1.1': 'APACHE_1_1',
                    'gpl-3.0': 'GPL_3_0',
                    'gpl-2.0': 'GPL_2_0',
                    'lgpl-3.0': 'LGPL_3_0',
                    'lgpl-2.1': 'LGPL_2_1'
                }
                if last in opensource_mapping:
                    return opensource_mapping[last]

            return None
        except Exception as e:
            log.debug(f"Failed to extract licence code from URL '{license_url}': {e}")
            return None

    def _map_creative_commons_segments(self, segments):
        """
        Map Creative Commons URL path segments to EU licence codes.
        """
        try:
            if not segments:
                return None

            if segments[0] == 'licenses' and len(segments) >= 3:
                variant = segments[1].lower()
                version = segments[2]
                territory = None
                if len(segments) >= 4:
                    extra = segments[3].lower()
                    if not extra.startswith('deed') and 'legalcode' not in extra:
                        territory = segments[3]

                variant_code = ''.join(part.upper() for part in variant.split('-'))
                version_code = version.replace('.', '_').upper()
                code = f'CC_{variant_code}'
                if version_code:
                    code += f'_{version_code}'
                if territory:
                    code += f'_{territory.replace("-", "_").upper()}'
                return code

            if segments[0] == 'publicdomain' and len(segments) >= 2:
                sub = segments[1].lower()
                if sub == 'zero':
                    return 'CC0'
                if sub == 'mark':
                    version = segments[2] if len(segments) >= 3 else ''
                    version_code = version.replace('.', '_').upper() if version else ''
                    code = 'CC_PDM'
                    if version_code:
                        code += f'_{version_code}'
                    return code

            return None
        except Exception as e:
            log.debug(f"Failed to map Creative Commons segments '{segments}': {e}")
            return None

    def _normalize_license_id_to_code(self, license_id):
        """
        Normalize CKAN licence IDs to EU licence codes when no URL is available.
        """
        if not license_id:
            return None

        normalized = license_id.strip().lower()
        mapping = {
            'cc-by': 'CC_BY_4_0',
            'cc-by-sa': 'CC_BYSA_4_0',
            'cc-by-nd': 'CC_BYND_4_0',
            'cc-by-nc': 'CC_BYNC_4_0',
            'cc-by-nc-sa': 'CC_BYNCSA_4_0',
            'cc-by-nc-nd': 'CC_BYNCND_4_0',
            'cc-zero': 'CC0',
            'cc0': 'CC0',
            'cc-nc': 'CC_BYNC_4_0',
            'odc-odbl': 'ODC_BL',
            'odc-pddl': 'ODC_PDDL',
            'odc-by': 'ODC_BY',
            'gfdl': 'GFDL_1_3',
            'gpl': 'GPL_3_0',
            'gpl-3.0': 'GPL_3_0',
            'gpl-2.0': 'GPL_2_0',
            'lgpl': 'LGPL_3_0',
            'lgpl-3.0': 'LGPL_3_0',
            'lgpl-2.1': 'LGPL_2_1',
            'mit': 'MIT',
            'apache': 'APACHE_2_0',
            'apache-2.0': 'APACHE_2_0',
            'apache-1.1': 'APACHE_1_1'
        }

        return mapping.get(normalized)
