# -*- coding: utf-8 -*-

import logging
import json
from typing import Optional
from ckan import model
from ckanext.dcat.harvesters.rdf import DCATRDFHarvester
from ckanext.harvest.interfaces import IHarvester
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.data_gov_gr import helpers as data_gov_helpers

log = logging.getLogger(__name__)

# Cache for vocabulary data to avoid repeated database queries
_vocabulary_cache = {}

class CustomDcatHarvester(DCATRDFHarvester, IHarvester):
    """
    Custom DCAT harvester for harvesting from data.gov.ie to data.gov.gr
    that fixes validation errors through custom mapping.
    """

    def _get_vocabulary_valid_codes(self, vocabulary_name):
        """
        Get valid codes from a controlled vocabulary in the database.
        Returns a set of valid codes (uppercase) that can be used in authority URIs.

        Args:
            vocabulary_name: Name of the vocabulary (e.g., 'Frequency', 'Licence')

        Returns:
            Set of valid uppercase codes
        """
        if not vocabulary_name:
            return set()

        alias_map = {
            'access right': 'Access right',
            'access rights': 'Access right',
            'data theme': 'Data theme',
            'dataset type': 'Dataset type',
            'frequency': 'Frequency',
            'high-value dataset categories': 'High-value dataset categories',
            'language': 'Languages',
            'languages': 'Languages',
            'licence': 'Licence',
            'license': 'Licence',
            'media type': 'Media types',
            'media types': 'Media types',
            'mimetype': 'Media types',
            'planned availability': 'Planned availability',
            'publisher type': 'Publisher type',
            'file type': 'File Type',
            'file type - non proprietary format': 'File Type - Non Proprietary Format',
            'machine readable file format': 'Machine Readable File Format'
        }

        lookup_name = alias_map.get(vocabulary_name.lower(), vocabulary_name)
        cache_key = f'valid_codes_{lookup_name}'

        if cache_key in _vocabulary_cache:
            return _vocabulary_cache[cache_key]

        try:
            vocabulary_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                {}, {'id': lookup_name}
            )
            tags = vocabulary_data.get('tags', [])

            # Extract codes from the vocabulary tags
            valid_codes = set()
            for tag in tags:
                # Try to get the code from value_uri or name
                if tag.get('value_uri'):
                    code = self._extract_code_from_identifier(tag['value_uri'])
                    if code:
                        valid_codes.add(code.upper())
                elif tag.get('name'):
                    code = self._extract_code_from_identifier(tag['name'])
                    if code:
                        valid_codes.add(code.upper())

            # Cache the result
            _vocabulary_cache[cache_key] = valid_codes
            log.debug(f"Loaded {len(valid_codes)} valid codes for vocabulary '{lookup_name}'")

            return valid_codes

        except toolkit.ObjectNotFound:
            log.warning(f"Vocabulary '{lookup_name}' not found in database")
            return set()
        except Exception as e:
            log.error(f"Error loading vocabulary '{vocabulary_name}': {e}", exc_info=True)
            return set()

    def _get_vocabulary_uri_map(self, vocabulary_name):
        """
        Return mapping CODE -> value (value_uri or name) for a controlled vocabulary.

        This uses the same alias resolution as _get_vocabulary_valid_codes and
        ensures that we always return the exact values that the scheming field
        expects for a given vocabulary entry.
        """
        if not vocabulary_name:
            return {}

        alias_map = {
            'access right': 'Access right',
            'access rights': 'Access right',
            'data theme': 'Data theme',
            'dataset type': 'Dataset type',
            'frequency': 'Frequency',
            'high-value dataset categories': 'High-value dataset categories',
            'language': 'Languages',
            'languages': 'Languages',
            'licence': 'Licence',
            'license': 'Licence',
            'media type': 'Media types',
            'media types': 'Media types',
            'mimetype': 'Media types',
            'planned availability': 'Planned availability',
            'publisher type': 'Publisher type',
            'file type': 'File Type',
            'file type - non proprietary format': 'File Type - Non Proprietary Format',
            'machine readable file format': 'Machine Readable File Format',
        }

        lookup_name = alias_map.get(vocabulary_name.lower(), vocabulary_name)
        try:
            data = toolkit.get_action('vocabularyadmin_vocabulary_show')({}, {'id': lookup_name})
            tags = data.get('tags', [])
        except Exception:
            log.warning(f"Could not load vocabulary '{lookup_name}' for URI map")
            return {}

        mapping = {}
        for tag in tags:
            if not isinstance(tag, dict):
                continue
            value_uri = tag.get('value_uri')
            name = tag.get('name')
            code = ''
            if value_uri:
                code = self._extract_code_from_identifier(value_uri)
            if not code and name:
                code = self._extract_code_from_identifier(name)
            if code:
                mapping[code.upper()] = value_uri or name

        return mapping

    def _extract_code_from_identifier(self, value):
        """
        Normalize a vocabulary identifier (URI or plain string) to a comparable code.
        """
        if not value or not isinstance(value, str):
            return ''

        trimmed = value.strip()
        if not trimmed:
            return ''

        lowered = trimmed.lower()
        if 'media-types/' in lowered:
            return trimmed.split('media-types/', 1)[-1].strip('/').strip()

        if trimmed.startswith('http://') or trimmed.startswith('https://'):
            return trimmed.rstrip('/').split('/')[-1]

        return trimmed

    def info(self):
        return {
            'name': 'custom_dcat_harvester',
            'title': 'DCAT Custom Harvester',
            'description': 'Custom DCAT harvester with advanced mapping and validation error fixes for data.gov.gr structure',
            'form_config_interface': 'Text',
            'show_config': False
        }

    def modify_package_dict(self, package_dict, temp_dict, harvest_object):
        """
        Apply custom mapping fixes to resolve validation errors
        """
        try:
            log.info(f"[DATA.GOV.GR HARVESTER] Applying custom mapping to dataset: {package_dict.get('name', 'unknown')}")
            log.info(f"[DATA.GOV.GR HARVESTER] Original frequency value: {package_dict.get('frequency', 'NOT SET')}")

            # Owner org is provided by each specific harvester (eg EKAN); no changes here

            # Extract source data from harvest object for metadata preservation
            source_data = self._extract_source_data_from_harvest_object(harvest_object)

            # Preserve key metadata that might be lost during mapping
            self._preserve_license_information(package_dict, source_data)
            self._preserve_contact_details(package_dict, source_data)

            # Fix 1: Handle missing Greek multilingual fields
            self._fix_multilingual_fields(package_dict)

            # Fix 2: Handle array-valued fields that should be single values
            self._fix_array_fields(package_dict)

            # Fix 3: Handle HVD category
            self._fix_hvd_category(package_dict)

            # Fix 4: Handle theme fields
            self._fix_theme_fields(package_dict)

            # Fix 5: Handle all authority URI fields (frequency, license, access_rights, etc.)
            self._fix_frequency_field(package_dict)
            self._fix_license_field(package_dict)
            self._fix_access_rights_field(package_dict)
            self._fix_availability_field(package_dict)

            # Fix 5.1: Handle mimetype field using controlled vocabulary
            self._fix_mimetype_field(package_dict)

            # Fix 5.2: Handle language field using controlled vocabulary
            self._fix_language_field(package_dict)

            log.info(f"[DATA.GOV.GR HARVESTER] After authority URI fixes: frequency={package_dict.get('frequency', 'NOT SET')}")

            # Fix 6: Ensure required translated fields exist for data.gov.gr
            self._fix_required_translated_fields(package_dict)

            # Fix 6.1: Normalise spatial_coverage, enrich WKT polygons with
            # bbox / centroid so that scheming + DCAT profiles can use them.
            self._fix_spatial_coverage(package_dict)

            # Fix 7: Clean tag validation issues
            self._fix_tag_validation(package_dict)

            # Fix 8: Handle resource validation issues
            self._fix_resource_validation(package_dict)

            # Fix 8.1: Handle resource mimetype validation using controlled vocabulary
            self._fix_resource_mimetype_fields(package_dict)

            # Fix 9: Preserve critical metadata from source
            self._preserve_resource_level_licenses(package_dict, source_data)
            self._extract_and_preserve_contact_phone(package_dict, source_data)

            # Fix 10: Ensure required field fallbacks exist
            self._ensure_required_fields(package_dict)

            # Fix 10: Handle data.gov.gr custom fields mapping
            # self._fix_custom_fields_mapping(package_dict)  # Method not implemented yet

            # Ensure access_rights and applicable_legislation for PUBLIC datasets
            self._ensure_access_rights_and_legislation(package_dict, source_data)

            log.info(f"Custom mapping applied to dataset: {package_dict.get('name', 'unknown')}")

        except Exception as e:
            log.error(f"Error applying custom mapping: {e}", exc_info=True)

        return package_dict

    

    def _extract_source_data_from_harvest_object(self, harvest_object):
        """
        Extract original source data for *this* dataset from harvest object content.

        For JSON-LD catalog sources (eg. POD data.json) the harvest object
        content is usually the whole catalog. In that case, try to locate the
        matching dataset entry using the harvest object's guid (typically the
        DCAT identifier). For non-JSON content (eg. Turtle, RDF/XML), just
        return an empty dict.
        """
        if not harvest_object or not hasattr(harvest_object, 'content') or not harvest_object.content:
            return {}

        try:
            raw = json.loads(harvest_object.content)
        except (json.JSONDecodeError, AttributeError):
            # Non-JSON content (eg RDF/XML, Turtle), nothing to extract
            log.debug("Harvest object content is not JSON; skipping source data extraction")
            return {}

        guid = getattr(harvest_object, 'guid', None)

        # If this looks like a DCAT/POD catalog, drill down to the dataset
        # that matches this harvest object's guid.
        if isinstance(raw, dict):
            datasets = None
            if isinstance(raw.get('dataset'), list):
                datasets = raw.get('dataset')
            elif isinstance(raw.get('@graph'), list):
                # Some JSON-LD feeds use @graph instead of dataset[]
                datasets = raw.get('@graph')

            if datasets and guid:
                for candidate in datasets:
                    if not isinstance(candidate, dict):
                        continue
                    ident = candidate.get('identifier') or candidate.get('id') or candidate.get('@id')

                    identifiers = []
                    if isinstance(ident, list):
                        identifiers = [str(v) for v in ident]
                    elif ident is not None:
                        identifiers = [str(ident)]

                    for value in identifiers:
                        # Match exact identifier or URIs that end with it
                        if value == guid or (value.endswith(guid) and guid):
                            return candidate

        # Fallback: return the raw JSON (may still be useful for helpers)
        return raw

    def _ensure_access_rights_and_legislation(self, dataset_dict, source_data):
        """
        Ensure access_rights and applicable_legislation are populated for harvested
        DCAT datasets.

        Logic:
        - access_rights:
          * If already set after mapping, keep it.
          * Else, if present in source_data, keep that.
          * Else, set to PUBLIC authority URI.
        - applicable_legislation:
          * If already set on dataset, keep it.
          * Else, if source_data provides applicable_legislation, copy it over.
          * Else, if access_rights is PUBLIC, use the configured open-data
            legislation (ckanext.data_gov_gr.dataset.legislation.open).
        """
        try:
            if not isinstance(dataset_dict, dict):
                return

            # 1) access_rights
            access_rights = dataset_dict.get('access_rights')
            if not access_rights and isinstance(source_data, dict):
                remote_access = source_data.get('access_rights')
                if isinstance(remote_access, str) and remote_access.strip():
                    dataset_dict['access_rights'] = remote_access.strip()
                    access_rights = dataset_dict['access_rights']

            if not dataset_dict.get('access_rights'):
                public_uri = 'http://publications.europa.eu/resource/authority/access-right/PUBLIC'
                dataset_dict['access_rights'] = public_uri
                access_rights = public_uri

            # 2) applicable_legislation
            existing = dataset_dict.get('applicable_legislation')
            if existing:
                return

            # Prefer values coming from the source dataset, if any
            remote_values = None
            if isinstance(source_data, dict):
                remote_values = source_data.get('applicable_legislation')

            values = None
            if isinstance(remote_values, list):
                values = [v.strip() for v in remote_values if isinstance(v, str) and v.strip()]
            elif isinstance(remote_values, str):
                candidate = remote_values.strip()
                if candidate:
                    values = [candidate]

            if values:
                dataset_dict['applicable_legislation'] = values
                return

            # Fallback: use configured open-data legislation when access_rights is PUBLIC
            if isinstance(access_rights, str):
                lowered = access_rights.strip().lower()
                if lowered.endswith('/public') or 'access-right/public' in lowered:
                    value = data_gov_helpers.get_config_value(
                        'ckanext.data_gov_gr.dataset.legislation.open', ''
                    )
                    if isinstance(value, str):
                        value = value.strip()
                        if value:
                            dataset_dict['applicable_legislation'] = [value]
        except Exception as e:
            log.error(f"Error ensuring access_rights/applicable_legislation: {e}", exc_info=True)

    def _preserve_license_information(self, dataset_dict, source_data):
        """
        Preserve license information that may be lost during harvesting
        """
        if not source_data:
            return

        if source_data.get('license_id'):
            dataset_dict['license_id'] = source_data['license_id']

    def _fix_spatial_coverage(self, dataset_dict):
        """Enrich spatial_coverage entries with bbox/centroid when possible.

        Many EKAN / NAP DCAT feeds provide a WKT POLYGON in the
        spatial_coverage text. To align with the scheming DCAT profile
        (which expects bbox/centroid fields), derive a simple envelope
        and centroid from rectangular polygons. Existing bbox/centroid
        values are left untouched.
        """
        spatial = dataset_dict.get('spatial_coverage')
        if not isinstance(spatial, list) or not spatial:
            return

        for item in spatial:
            if not isinstance(item, dict):
                continue

            # Skip if already enriched
            if any(item.get(k) for k in ('bbox', 'centroid', 'geom')):
                continue

            text = item.get('text')
            if not isinstance(text, str):
                continue

            coords = self._parse_wkt_polygon(text)
            if not coords:
                continue

            xs = [x for x, _ in coords]
            ys = [y for _, y in coords]
            minx, maxx = min(xs), max(xs)
            miny, maxy = min(ys), max(ys)

            bbox = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [minx, miny],
                        [minx, maxy],
                        [maxx, maxy],
                        [maxx, miny],
                        [minx, miny],
                    ]
                ],
            }
            centroid = {
                "type": "Point",
                "coordinates": [
                    (minx + maxx) / 2.0,
                    (miny + maxy) / 2.0,
                ],
            }

            try:
                item['bbox'] = json.dumps(bbox, ensure_ascii=False)
                item['centroid'] = json.dumps(centroid, ensure_ascii=False)
                # Preserve original WKT as geom and use a friendly label
                item['geom'] = text.strip()
                if text.strip().upper().startswith('POLYGON'):
                    item['text'] = 'Γεωγραφική περιοχή'
            except Exception as e:
                log.warning(f"Error normalising spatial_coverage: {e}")

    def _parse_wkt_polygon(self, wkt_text):
        if not wkt_text or not isinstance(wkt_text, str):
            return None
        s = wkt_text.strip()
        if not s.upper().startswith('POLYGON'):
            return None
        try:
            start = s.find('((')
            end = s.rfind('))')
            if start == -1 or end == -1 or end <= start + 2:
                return None
            inner = s[start + 2 : end]
            coords = []
            for part in inner.split(','):
                part = part.strip()
                if not part:
                    continue
                tokens = part.split()
                if len(tokens) < 2:
                    continue
                x = float(tokens[0])
                y = float(tokens[1])
                coords.append((x, y))
            return coords or None
        except Exception:
            return None
            log.debug(f"Preserved license_id from source: {source_data['license_id']}")

        if source_data.get('license_title'):
            dataset_dict['license_title'] = source_data['license_title']
            log.debug(f"Preserved license_title from source: {source_data['license_title']}")

        if source_data.get('license_url'):
            dataset_dict['license_url'] = source_data['license_url']

        # Fallback for open datasets
        if not dataset_dict.get('license_id') and source_data.get('isopen'):
            common_licenses = {
                'CC-BY-4.0': 'Creative Commons Attribution 4.0',
                'CC0-1.0': 'Creative Commons CC0 1.0 Universal',
                'ODC-BY-1.0': 'Open Data Commons Attribution License',
                'OGL-UK-3.0': 'Open Government Licence v3.0',
                'MIT': 'MIT License'
            }

            for license_id, license_title in common_licenses.items():
                if source_data.get('license_title') == license_title:
                    dataset_dict['license_id'] = license_id
                    dataset_dict['license_title'] = license_title
                    break

    def _preserve_contact_details(self, dataset_dict, source_data):
        """
        Preserve contact information sourced from the original dataset
        """
        if not source_data:
            return

        contact_fields = ['contact_name', 'contact_email', 'contact_phone', 'maintainer', 'maintainer_email']
        for field in contact_fields:
            if source_data.get(field):
                dataset_dict[field] = source_data[field]
                log.debug(f"Preserved {field}: {source_data[field]}")

        extras = dataset_dict.setdefault('extras', [])
        for field in contact_fields:
            if source_data.get(field):
                existing_extra = next((e for e in extras if e.get('key') == field), None)
                if not existing_extra:
                    extras.append({'key': field, 'value': source_data[field]})

    def _fix_multilingual_fields(self, dataset_dict):
        """
        Fix missing Greek multilingual fields by falling back to English
        """
        # Handle title translated fields (required by data.gov.gr schema)
        if dataset_dict.get('title_translated'):
            if not dataset_dict['title_translated'].get('el'):
                # Use English as fallback for missing Greek
                if dataset_dict['title_translated'].get('en'):
                    dataset_dict['title_translated']['el'] = dataset_dict['title_translated']['en']
                else:
                    # Use the main title as fallback
                    if dataset_dict.get('title'):
                        dataset_dict['title_translated']['el'] = dataset_dict['title']

        # Handle notes translated fields (required by data.gov.gr schema)
        if dataset_dict.get('notes_translated'):
            if not dataset_dict['notes_translated'].get('el'):
                # Use English as fallback for missing Greek
                if dataset_dict['notes_translated'].get('en'):
                    dataset_dict['notes_translated']['el'] = dataset_dict['notes_translated']['en']
                else:
                    # Use the main notes as fallback
                    if dataset_dict.get('notes'):
                        dataset_dict['notes_translated']['el'] = dataset_dict['notes']

        # Ensure title and notes exist for the main fields
        if not dataset_dict.get('title') and dataset_dict.get('title_translated'):
            # Try Greek first, then English
            if dataset_dict['title_translated'].get('el'):
                dataset_dict['title'] = dataset_dict['title_translated']['el']
            elif dataset_dict['title_translated'].get('en'):
                dataset_dict['title'] = dataset_dict['title_translated']['en']

        if not dataset_dict.get('notes') and dataset_dict.get('notes_translated'):
            # Try Greek first, then English
            if dataset_dict['notes_translated'].get('el'):
                dataset_dict['notes'] = dataset_dict['notes_translated']['el']
            elif dataset_dict['notes_translated'].get('en'):
                dataset_dict['notes'] = dataset_dict['notes_translated']['en']

        # Remove empty translated fields to avoid validation errors
        if 'title_translated' in dataset_dict and not dataset_dict['title_translated']:
            del dataset_dict['title_translated']
        if 'notes_translated' in dataset_dict and not dataset_dict['notes_translated']:
            del dataset_dict['notes_translated']

    def _fix_array_fields(self, dataset_dict):
        """
        Fix fields that come as JSON strings but should be arrays
        data.gov.gr stores HVD category and dcat_type as arrays
        """
        # Handle HVD category - keep as array
        if 'hvd_category' in dataset_dict:
            hvd_value = dataset_dict['hvd_category']
            if isinstance(hvd_value, str):
                try:
                    # Parse JSON array and keep all values
                    hvd_array = json.loads(hvd_value)
                    if isinstance(hvd_array, list) and hvd_array:
                        dataset_dict['hvd_category'] = hvd_array
                        log.debug(f"Parsed HVD category JSON array: {len(hvd_array)} items")
                except (json.JSONDecodeError, IndexError):
                    # If parsing fails, remove the field
                    del dataset_dict['hvd_category']
                    log.warning(f"Failed to parse HVD category JSON: {hvd_value}")
            elif isinstance(hvd_value, list):
                # Already an array - keep it
                log.debug(f"HVD category is already an array: {len(hvd_value)} items")

        # Handle dcat_type - keep as array
        if 'dcat_type' in dataset_dict:
            dcat_type_value = dataset_dict['dcat_type']
            if isinstance(dcat_type_value, str):
                try:
                    # Parse JSON array and keep all values
                    dcat_type_array = json.loads(dcat_type_value)
                    if isinstance(dcat_type_array, list) and dcat_type_array:
                        dataset_dict['dcat_type'] = dcat_type_array
                        log.debug(f"Parsed dcat_type JSON array: {len(dcat_type_array)} items")
                except (json.JSONDecodeError, IndexError):
                    # If parsing fails, remove the field
                    del dataset_dict['dcat_type']
                    log.warning(f"Failed to parse dcat_type JSON: {dcat_type_value}")
            elif isinstance(dcat_type_value, list):
                # Already an array - keep it
                log.debug(f"dcat_type is already an array: {len(dcat_type_value)} items")

    def _fix_hvd_category(self, dataset_dict):
        """
        Handle HVD category field specifically
        data.gov.gr stores hvd_category as an array of authority URIs
        """
        # Check if HVD category exists in main fields
        if 'hvd_category' in dataset_dict:
            hvd_value = dataset_dict['hvd_category']

            # If it's already a list, validate and clean the URIs
            if isinstance(hvd_value, list):
                cleaned_hvd = []
                for uri in hvd_value:
                    if isinstance(uri, str) and 'data.europa.eu/bna/' in uri:
                        cleaned_hvd.append(uri)
                    else:
                        log.warning(f"Invalid HVD category URI: {uri}")

                if cleaned_hvd:
                    dataset_dict['hvd_category'] = cleaned_hvd
                    log.debug(f"HVD category array validated: {len(cleaned_hvd)} valid URIs")
                else:
                    del dataset_dict['hvd_category']
                    log.warning("No valid HVD category URIs found, removing field")

            elif isinstance(hvd_value, str):
                try:
                    # Parse JSON array and keep all values
                    hvd_array = json.loads(hvd_value)
                    if isinstance(hvd_array, list) and hvd_array:
                        # Validate URIs
                        cleaned_hvd = [uri for uri in hvd_array if isinstance(uri, str) and 'data.europa.eu/bna/' in uri]
                        if cleaned_hvd:
                            dataset_dict['hvd_category'] = cleaned_hvd
                            log.debug(f"Fixed HVD category from JSON array: {len(cleaned_hvd)} valid URIs")
                        else:
                            del dataset_dict['hvd_category']
                except (json.JSONDecodeError, IndexError):
                    # If not JSON, check if it's a single authority URI
                    if 'data.europa.eu/bna/' in hvd_value:
                        # Convert single URI to array
                        dataset_dict['hvd_category'] = [hvd_value]
                        log.debug(f"Converted single HVD category URI to array: {hvd_value}")
                    else:
                        # If parsing fails and not valid URI, remove the field
                        del dataset_dict['hvd_category']
                        log.warning(f"Removed invalid HVD category: {hvd_value}")

        # Also check for HVD category in extras
        for extra in dataset_dict.get('extras', []):
            if extra['key'] == 'hvd_category':
                hvd_value = extra['value']
                if isinstance(hvd_value, str):
                    try:
                        # Parse JSON array and keep all values
                        hvd_array = json.loads(hvd_value)
                        if isinstance(hvd_array, list) and hvd_array:
                            extra['value'] = hvd_array
                            log.debug(f"Fixed HVD category from extras JSON array: {len(hvd_array)} items")
                    except (json.JSONDecodeError, IndexError):
                        # If not JSON, check if it's a single authority URI
                        if 'data.europa.eu/bna/' in hvd_value:
                            extra['value'] = [hvd_value]
                            log.debug(f"Converted single HVD category in extras to array")
                        else:
                            # If parsing fails and not valid URI, remove this extra
                            dataset_dict['extras'].remove(extra)
                            log.warning(f"Removed invalid HVD category from extras: {hvd_value}")
                break

    def _fix_theme_fields(self, dataset_dict):
        """
        Handle theme fields that come as arrays or authority URIs
        """
        # Find theme in extras
        for extra in dataset_dict.get('extras', []):
            if extra['key'] == 'theme':
                theme_value = extra['value']
                if isinstance(theme_value, str):
                    try:
                        # Parse JSON array and take first value
                        theme_array = json.loads(theme_value)
                        if isinstance(theme_array, list) and theme_array:
                            # Extract theme URI from complex array structure
                            first_theme = theme_array[0]
                            if isinstance(first_theme, str):
                                if first_theme.startswith('[') and first_theme.endswith(']'):
                                    # Handle nested JSON structure like '["uri", "label"]'
                                    nested_array = json.loads(first_theme)
                                    if isinstance(nested_array, list) and nested_array:
                                        extra['value'] = nested_array[0]
                                    else:
                                        extra['value'] = first_theme
                                else:
                                    extra['value'] = first_theme
                            else:
                                extra['value'] = str(first_theme)
                            log.debug(f"Fixed theme from extras: {theme_value} -> {extra['value']}")
                    except (json.JSONDecodeError, IndexError):
                        # If not JSON, check if it's already a valid authority URI
                        if 'authority/data-theme/' in theme_value:
                            log.debug(f"Theme in extras is already a valid authority URI: {theme_value}")
                        else:
                            # If parsing fails and not valid URI, remove this extra
                            dataset_dict['extras'].remove(extra)
                            log.debug(f"Removed invalid theme from extras: {theme_value}")
                break

        # Also check for theme in main dataset fields
        if 'theme' in dataset_dict:
            theme_value = dataset_dict['theme']

            # Handle theme as array (data.gov.gr stores themes as arrays)
            if isinstance(theme_value, list) and theme_value:
                # Keep all themes as array (data.gov.gr structure)
                cleaned_themes = []
                for theme in theme_value:
                    if isinstance(theme, str) and 'authority/data-theme/' in theme:
                        cleaned_themes.append(theme)
                    elif isinstance(theme, str):
                        cleaned_themes.append(theme)

                if cleaned_themes:
                    dataset_dict['theme'] = cleaned_themes
                    log.debug(f"Theme is array, keeping all {len(cleaned_themes)} authority URIs")
                else:
                    # Fallback to first theme if cleaning failed
                    dataset_dict['theme'] = [theme_value[0]]

            elif isinstance(theme_value, str):
                try:
                    # Parse JSON array and take first value
                    theme_array = json.loads(theme_value)
                    if isinstance(theme_array, list) and theme_array:
                        first_theme = theme_array[0]
                        if isinstance(first_theme, str):
                            if first_theme.startswith('[') and first_theme.endswith(']'):
                                # Handle nested JSON structure
                                nested_array = json.loads(first_theme)
                                if isinstance(nested_array, list) and nested_array:
                                    dataset_dict['theme'] = nested_array[0]
                                else:
                                    dataset_dict['theme'] = first_theme
                            else:
                                dataset_dict['theme'] = first_theme
                        else:
                            dataset_dict['theme'] = str(first_theme)
                        log.debug(f"Fixed theme from JSON: {theme_value} -> {dataset_dict['theme']}")
                except (json.JSONDecodeError, IndexError):
                    # If not JSON, check if it's already a valid authority URI
                    if 'authority/data-theme/' in theme_value:
                        log.debug(f"Theme is already a valid authority URI: {theme_value}")
                    else:
                        # If parsing fails and not valid URI, will move to tags below
                        pass

        # Move any remaining theme values to tags to avoid controlled vocabulary errors
        if 'theme' in dataset_dict:
            values = dataset_dict['theme']
            if not isinstance(values, list):
                values = [values]

            # Prepare tag list on the dataset
            existing_tags = set()
            for t in dataset_dict.get('tags', []) or []:
                if isinstance(t, dict) and 'name' in t and t['name']:
                    existing_tags.add(t['name'].strip().lower())

            for tv in values:
                if not isinstance(tv, str) or not tv.strip():
                    continue
                label = tv
                # If it's an authority URI, use the last segment as tag label
                if 'authority/data-theme/' in tv:
                    label = tv.rsplit('/', 1)[-1]
                clean = label.strip()
                if clean and clean.lower() not in existing_tags:
                    dataset_dict.setdefault('tags', []).append({'name': clean})
                    existing_tags.add(clean.lower())

            # Remove theme field entirely to avoid "unexpected choice" validation
            del dataset_dict['theme']
            log.debug('Moved theme values to tags and removed theme field to satisfy controlled vocabulary')

    def _fix_authority_uri_field(self, dataset_dict, field_name, uri_base, valid_codes):
        """
        Generic method to handle authority URI fields dynamically.

        Args:
            dataset_dict: The dataset dictionary
            field_name: The field name (e.g., 'frequency', 'license', 'theme')
            uri_base: The base URI (e.g., 'http://publications.europa.eu/resource/authority/frequency/')
            valid_codes: Set of valid codes for this vocabulary
        """
        if field_name not in dataset_dict:
            return

        if not valid_codes:
            log.debug(f"[{field_name.upper()}] No controlled vocabulary codes found, skipping normalization.")
            return

        value = dataset_dict[field_name]

        # If it's already a valid authority URI, keep it
        if isinstance(value, str) and uri_base in value:
            log.debug(f"{field_name} is already a valid authority URI: {value}")
            return

        # Handle arrays (e.g., theme can be array)
        if isinstance(value, list):
            cleaned_values = []
            for item in value:
                if isinstance(item, str):
                    if uri_base in item:
                        cleaned_values.append(item)
                        continue

                    item_code_original = self._extract_code_from_identifier(item)
                    item_code = item_code_original.upper()
                    if item_code and item_code in valid_codes:
                        cleaned_values.append(f"{uri_base}{item_code}")
                        log.info(f"[{field_name.upper()}] Dynamically mapped: '{item}' -> '{uri_base}{item_code}'")
                    else:
                        log.debug(f"[{field_name.upper()}] Skipping unmapped value '{item}' (normalized: '{item_code}')")

            if cleaned_values:
                dataset_dict[field_name] = cleaned_values
            else:
                del dataset_dict[field_name]
            return

        # Handle single string value
        if isinstance(value, str):
            value_code_original = self._extract_code_from_identifier(value)
            value_code = value_code_original.upper()

            if value_code and value_code in valid_codes:
                dataset_dict[field_name] = f"{uri_base}{value_code}"
                log.info(f"[{field_name.upper()}] Dynamically mapped: '{value}' -> '{dataset_dict[field_name]}'")
            elif not value_code:
                del dataset_dict[field_name]
            else:
                log.debug(f"[{field_name.upper()}] Removing unmapped value '{value}' (normalized: '{value_code}')")
                del dataset_dict[field_name]

    def _fix_frequency_field(self, dataset_dict):
        """
        Handle frequency field mapping using dynamic URI construction.
        Loads valid codes from the 'Frequency' vocabulary in the database.
        """
        # Convert frequency to uppercase before processing
        if 'frequency' in dataset_dict:
            frequency = dataset_dict['frequency']
            if isinstance(frequency, str):
                # Convert to uppercase - this handles "other" -> "OTHER", etc.
                dataset_dict['frequency'] = frequency.strip().upper()

        # (EKAN-specific mapping is handled upstream; no ISO mapping here)

        # Get valid codes from database vocabulary
        valid_frequency_codes = self._get_vocabulary_valid_codes('Frequency')

        # Use generic method
        self._fix_authority_uri_field(
            dataset_dict,
            'frequency',
            'http://publications.europa.eu/resource/authority/frequency/',
            valid_frequency_codes
        )

    def _fix_license_field(self, dataset_dict):
        """
        Handle license field mapping using dynamic URI construction.
        Loads valid codes from the 'Licence' vocabulary in the database.
        """
        # Handle alternative spellings
        if 'license' in dataset_dict or 'license_id' in dataset_dict:
            license_value = dataset_dict.get('license') or dataset_dict.get('license_id')
            if license_value and isinstance(license_value, str):
                lic_lower = license_value.strip().lower().replace('.', '_').replace('-', '_')
                alternative_mappings = {
                    'cc0': 'CC0_1_0',
                    'cc_by': 'CC_BY_4_0',
                    'cc_by_sa': 'CC_BY_SA_4_0',
                    'cc_by_nc': 'CC_BY_NC_4_0',
                    'cc_by_nd': 'CC_BY_ND_4_0',
                    'cc_by_nc_sa': 'CC_BY_NC_SA_4_0',
                    'cc_by_nc_nd': 'CC_BY_NC_ND_4_0',
                    'odc_by': 'ODC_BY',
                    'odc_odbl': 'ODC_ODBL',
                    'odc_pddl': 'ODC_PDDL',
                    'mit': 'MIT',
                    'gpl': 'GPL_3_0',
                    'lgpl': 'LGPL_3_0',
                    'apache': 'APL_2_0',
                }
                if lic_lower in alternative_mappings:
                    # Use the main 'license' field
                    dataset_dict['license'] = alternative_mappings[lic_lower]

        # Get valid codes from database vocabulary
        valid_licence_codes = self._get_vocabulary_valid_codes('Licence')

        # Try both 'license' and 'license_id' fields
        for field_name in ['license', 'license_id']:
            self._fix_authority_uri_field(
                dataset_dict,
                field_name,
                'http://publications.europa.eu/resource/authority/licence/',
                valid_licence_codes
            )

    def _fix_access_rights_field(self, dataset_dict):
        """
        Handle access_rights field mapping using dynamic URI construction.
        Loads valid codes from the 'Access right' vocabulary in the database.
        """
        # Get valid codes from database vocabulary
        valid_access_rights_codes = self._get_vocabulary_valid_codes('Access right')

        self._fix_authority_uri_field(
            dataset_dict,
            'access_rights',
            'http://publications.europa.eu/resource/authority/access-right/',
            valid_access_rights_codes
        )

    def _fix_availability_field(self, dataset_dict):
        """
        Handle availability field mapping using dynamic URI construction.
        Loads valid codes from the 'Planned availability' vocabulary in the database.
        """
        # Get valid codes from database vocabulary
        valid_availability_codes = self._get_vocabulary_valid_codes('Planned availability')

        self._fix_authority_uri_field(
            dataset_dict,
            'availability',
            'http://publications.europa.eu/resource/authority/planned-availability/',
            valid_availability_codes
        )



    def _fix_mimetype_field(self, dataset_dict):
        """
        Handle mimetype field mapping using controlled vocabulary.

        In the DCAT-AP GR profiles used on data.gov.gr the mimetype
        field is defined on resources, not at dataset level, so we
        delegate to the resource-level helper.
        """
        if not isinstance(dataset_dict, dict):
            return

        # Ensure resource-level mimetype values are checked against the
        # 'Media types' vocabulary and either normalised or preserved
        # as fallbacks when not in the vocabulary.
        self._fix_resource_mimetype_fields(dataset_dict)

    def _fix_language_field(self, dataset_dict):
        """
        Handle language field mapping using controlled vocabulary.
        Loads valid codes from the 'Languages' vocabulary in the database.
        Handles language fields that come as JSON strings in extras.
        """
        # Get valid codes from database vocabulary
        valid_language_codes = self._get_vocabulary_valid_codes('Languages')

        def normalize_language_value(value: str) -> Optional[str]:
            if not value or not isinstance(value, str):
                return None
            candidate = value.strip()
            if not candidate:
                return None

            alias_map = {
                'EN': 'ENG',
                'EL': 'ELL',
                'GR': 'ELL',
            }

            if 'publications.europa.eu' in candidate:
                code = candidate.split('/')[-1].upper()
                code = alias_map.get(code, code)
                if code not in valid_language_codes:
                    return None
                base = candidate.rsplit('/', 1)[0]
                return f"{base}/{code}"

            code = alias_map.get(candidate.upper(), candidate.upper())
            if code not in valid_language_codes:
                return None
            return f"https://publications.europa.eu/resource/authority/language/{code}"

        # Handle language in extras (where it usually ends up from RDF)
        for extra in dataset_dict.get('extras', []):
            if extra['key'] == 'language':
                language_value = extra['value']

                if isinstance(language_value, str):
                    # Try to parse JSON array format like "[\"http://.../ENG\"]"
                    try:
                        language_array = json.loads(language_value)
                        if isinstance(language_array, list) and language_array:
                            # Take the first language URI from the array
                            language_uri = language_array[0]
                            if isinstance(language_uri, str):
                                # Extract code from URI (last part after /)
                                normalized_uri = normalize_language_value(language_uri)

                                if normalized_uri:
                                    dataset_dict['language'] = normalized_uri
                                    log.info(f"[LANGUAGE] Moved valid language from extras: '{normalized_uri}'")
                                else:
                                    log.warning(f"[LANGUAGE] Language value '{language_uri}' not in controlled vocabulary")
                                dataset_dict['extras'].remove(extra)
                    except (json.JSONDecodeError, IndexError):
                        # If not JSON, check if it's already a valid authority URI
                        normalized_uri = normalize_language_value(language_value)
                        if normalized_uri:
                            dataset_dict['language'] = normalized_uri
                            log.info(f"[LANGUAGE] Moved valid language URI from extras: '{normalized_uri}'")
                        else:
                            log.warning(f"[LANGUAGE] Invalid language format in extras: {language_value}")
                        dataset_dict['extras'].remove(extra)
                break

        # Also check if language exists in main dataset fields
        if 'language' in dataset_dict:
            language_value = dataset_dict['language']

            if isinstance(language_value, str):
                # Handle array format in main field too
                try:
                    language_array = json.loads(language_value)
                    if isinstance(language_array, list) and language_array:
                        language_uri = language_array[0]
                        if isinstance(language_uri, str):
                            normalized_uri = normalize_language_value(language_uri)
                            if normalized_uri:
                                dataset_dict['language'] = normalized_uri
                                log.info(f"[LANGUAGE] Fixed language in main field: '{normalized_uri}'")
                            else:
                                log.warning(f"[LANGUAGE] Language value '{language_uri}' not in controlled vocabulary. Removing field.")
                                del dataset_dict['language']
                except (json.JSONDecodeError, IndexError):
                    normalized_uri = normalize_language_value(language_value)
                    if normalized_uri:
                        dataset_dict['language'] = normalized_uri
                        log.info(f"[LANGUAGE] Normalized language in main field: '{normalized_uri}'")
                    else:
                        log.warning(f"[LANGUAGE] Invalid language format in main field: {language_value}")
                        del dataset_dict['language']

    def _fix_resource_mimetype_fields(self, dataset_dict):
        """
        Normalise resource mimetype values against the 'Media types' vocabulary.

        - Accepts short codes (eg CSV), tokens (eg text/csv) or full IANA URLs.
        - Converts valid values to the exact vocabulary value (value_uri or name),
          so that scheming validation passes.
        - When a value cannot be mapped to the vocabulary, removes it
          from the resource but preserves it at dataset level via
          extras and tags so the information is not lost.
        """
        if 'resources' not in dataset_dict or not dataset_dict['resources']:
            return

        def _record_unmapped_mimetype(raw_value, code=None):
            """
            Record an unmapped mimetype as a non-blocking fallback:
            - add a tag with the extracted code (or original value)
            """
            if not raw_value or not isinstance(raw_value, str):
                return

            value = raw_value.strip()
            if not value:
                return

            # Record in tags
            tag_label_source = (code or value).strip()
            if not tag_label_source:
                return
            tag_label = tag_label_source.lower()

            tags = dataset_dict.get('tags')
            if not isinstance(tags, list):
                tags = []
            existing = set()
            for tag in tags:
                if isinstance(tag, dict):
                    name = tag.get('name')
                    if isinstance(name, str):
                        existing.add(name.strip().lower())

            if tag_label not in existing:
                tags.append({'name': tag_label})
                dataset_dict['tags'] = tags

        media_uri_map = self._get_vocabulary_uri_map('Media types')
        if not media_uri_map:
            # If vocabulary can't be loaded, drop mimetype to avoid validation errors
            for resource in dataset_dict['resources']:
                if isinstance(resource, dict):
                    value = resource.get('mimetype')
                    if isinstance(value, str) and value.strip():
                        log.warning(
                            "[RESOURCE MIMETYPE] Dropping mimetype '%s' for resource '%s' "
                            "because 'Media types' vocabulary could not be loaded",
                            value,
                            resource.get('name', 'unnamed'),
                        )
                        _record_unmapped_mimetype(value)
                    resource.pop('mimetype', None)
            return

        for resource in dataset_dict['resources']:
            if not isinstance(resource, dict):
                continue

            value = resource.get('mimetype')
            if not isinstance(value, str) or not value.strip():
                if 'mimetype' in resource and not value:
                    resource.pop('mimetype', None)
                continue

            raw_value = value.strip()
            code = self._extract_code_from_identifier(raw_value)
            if not code:
                _record_unmapped_mimetype(raw_value)
                resource.pop('mimetype', None)
                continue

            uri_value = media_uri_map.get(code.upper())
            if uri_value:
                resource['mimetype'] = uri_value
                log.info(
                    "[RESOURCE MIMETYPE] Normalised mimetype for resource '%s' to '%s' (code '%s')",
                    resource.get('name', 'unnamed'),
                    uri_value,
                    code.upper(),
                )
            else:
                _record_unmapped_mimetype(raw_value, code.upper())
                resource.pop('mimetype', None)

    def _fix_required_translated_fields(self, dataset_dict):
        """
        Ensure required translated fields exist for data.gov.gr validation
        """
        # Ensure title_translated-el exists
        if not dataset_dict.get('title_translated-el'):
            if dataset_dict.get('title_translated', {}).get('el'):
                dataset_dict['title_translated-el'] = dataset_dict['title_translated']['el']
            elif dataset_dict.get('title'):
                dataset_dict['title_translated-el'] = dataset_dict['title']
            else:
                dataset_dict['title_translated-el'] = 'Untitled Dataset'

        # Ensure notes_translated-el exists
        if not dataset_dict.get('notes_translated-el'):
            if dataset_dict.get('notes_translated', {}).get('el'):
                dataset_dict['notes_translated-el'] = dataset_dict['notes_translated']['el']
            elif dataset_dict.get('notes'):
                dataset_dict['notes_translated-el'] = dataset_dict['notes']
            else:
                dataset_dict['notes_translated-el'] = 'Dataset description'

        log.debug(f"Set required translated fields: title={dataset_dict.get('title_translated-el', 'N/A')[:50]}..., notes={dataset_dict.get('notes_translated-el', 'N/A')[:50]}...")

    def _fix_tag_validation(self, dataset_dict):
        """
        Fix tag validation issues by cleaning invalid characters
        """
        if 'tags' not in dataset_dict:
            return

        cleaned_tags = []
        for tag in dataset_dict['tags']:
            if isinstance(tag, dict) and 'name' in tag:
                original_name = tag['name']
                # Remove invalid characters and replace with valid alternatives
                cleaned_name = original_name
                cleaned_name = cleaned_name.replace('(', '-')  # Replace parentheses with hyphens
                cleaned_name = cleaned_name.replace(')', '-')
                cleaned_name = cleaned_name.replace('"', '')  # Remove quotes
                cleaned_name = cleaned_name.replace("'", '')  # Remove apostrophes

                # Only keep alphanumeric characters, spaces, hyphens, underscores, and dots
                cleaned_name = ''.join(char for char in cleaned_name if char.isalnum() or char in ' -_.')

                # Remove multiple consecutive spaces and trim
                cleaned_name = ' '.join(cleaned_name.split())

                if cleaned_name and cleaned_name != original_name:
                    log.debug(f"Cleaned tag: '{original_name}' -> '{cleaned_name}'")
                    cleaned_tags.append({'name': cleaned_name})
                elif cleaned_name:
                    cleaned_tags.append(tag)
                else:
                    log.warning(f"Removed invalid tag: '{original_name}'")
            else:
                log.warning(f"Invalid tag format: {tag}")

        dataset_dict['tags'] = cleaned_tags

    def _ensure_required_fields(self, dataset_dict):
        """
        Ensure translated field fallbacks exist for validation
        """
        if not dataset_dict.get('title_translated'):
            dataset_dict['title_translated'] = {
                'el': dataset_dict.get('title', 'Untitled'),
                'en': dataset_dict.get('title', 'Untitled')
            }

        if not dataset_dict.get('notes_translated'):
            dataset_dict['notes_translated'] = {
                'el': dataset_dict.get('notes', 'Dataset description'),
                'en': dataset_dict.get('notes', 'Dataset description')
            }

        if not dataset_dict.get('title_translated-el'):
            if dataset_dict.get('title_translated', {}).get('el'):
                dataset_dict['title_translated-el'] = dataset_dict['title_translated']['el']
            else:
                dataset_dict['title_translated-el'] = dataset_dict.get('title', 'Untitled Dataset')

        if not dataset_dict.get('notes_translated-el'):
            if dataset_dict.get('notes_translated', {}).get('el'):
                dataset_dict['notes_translated-el'] = dataset_dict['notes_translated']['el']
            else:
                dataset_dict['notes_translated-el'] = dataset_dict.get('notes', 'Dataset description')

        log.debug(f"Ensured required translated fields for: {dataset_dict.get('name', 'unknown')}")

    def _fix_resource_validation(self, dataset_dict):
        """
        Fix resource validation issues by ensuring required fields exist and are valid
        """
        if 'resources' not in dataset_dict or not dataset_dict['resources']:
            log.warning(f"No resources found for dataset: {dataset_dict.get('name', 'unknown')}")
            return

        cleaned_resources = []
        for resource in dataset_dict['resources']:
            if not isinstance(resource, dict):
                log.warning(f"Invalid resource format (not a dict): {resource}")
                continue

            # Ensure required resource fields exist
            if not resource.get('url'):
                fallback_url = (
                    resource.get('download_url')
                    or resource.get('access_url')
                    or resource.get('foaf_page')
                    or resource.get('uri')
                )
                if fallback_url:
                    resource['url'] = fallback_url
                else:
                    log.warning(f"Resource missing URL, skipping: {resource.get('name', 'unnamed')}")
                    continue

            # Fix resource name - required field
            if not resource.get('name') or not resource['name'].strip():
                if resource.get('url'):
                    # Generate name from URL
                    url = resource['url']
                    resource['name'] = url.split('/')[-1] or f"Resource_{url[:20]}"
                else:
                    resource['name'] = "Unnamed Resource"

            # Clean and validate resource name
            resource['name'] = resource['name'].strip()
            if len(resource['name']) > 100:
                resource['name'] = resource['name'][:100]

            # Fix resource format/mimetype
            # First check if format is empty or null
            if not resource.get('format') or not resource['format'].strip():
                # Try to guess format from URL
                url = resource.get('url', '').lower()

                # Enhanced format detection
                if '/csv/' in url or url.endswith('.csv'):
                    resource['format'] = 'CSV'
                elif '/json-stat/' in url:
                    resource['format'] = 'JSON-stat'
                elif '/json/' in url or url.endswith('.json'):
                    resource['format'] = 'JSON'
                elif '/xlsx/' in url or url.endswith('.xlsx'):
                    resource['format'] = 'XLSX'
                elif '/px/' in url:
                    resource['format'] = 'PX'
                elif url.endswith('.xml'):
                    resource['format'] = 'XML'
                elif url.endswith('.kml'):
                    resource['format'] = 'KML'
                elif url.endswith('.zip'):
                    resource['format'] = 'ZIP'
                elif url.endswith('.geojson'):
                    resource['format'] = 'GeoJSON'
                elif url.endswith('.shp'):
                    resource['format'] = 'SHP'
                elif 'wms' in url or 'wfs' in url:
                    resource['format'] = 'WMS' if 'wms' in url else 'WFS'
                elif 'arcgis' in url and 'rest' in url:
                    resource['format'] = 'ArcGIS REST'
                elif 'api' in url:
                    resource['format'] = 'API'
                elif url.endswith('.html') or url.endswith('.htm'):
                    resource['format'] = 'HTML'
                else:
                    resource['format'] = 'Unknown'
            else:
                # Format exists, just normalize it
                format_value = resource['format'].strip()

                # Keep valid formats as-is (case-insensitive check, but preserve original case)
                valid_formats = {
                    'csv', 'json-stat', 'json', 'xlsx', 'px', 'xml', 'kml', 'zip',
                    'geojson', 'shp', 'html', 'wms', 'wfs', 'pdf', 'rdf', 'ttl',
                    'arcgis rest', 'api', 'txt', 'doc', 'docx', 'xls'
                }

                if '://' in format_value:
                    # Treat as URI - derive short format code
                    uri_code = self._extract_code_from_identifier(format_value)
                    if uri_code:
                        resource['format'] = uri_code.upper()[:50]
                    else:
                        resource['format'] = format_value[:50]
                elif format_value.lower() in valid_formats:
                    # Keep the format but ensure proper capitalization
                    resource['format'] = format_value.upper()
                elif format_value.lower() == 'json-stat':
                    resource['format'] = 'JSON-stat'  # Special case for hyphenated format
                elif '/' in format_value and format_value.count('/') == 1:
                    resource['format'] = format_value.split('/')[-1].upper()[:50]
                else:
                    # Unknown format - convert to uppercase
                    resource['format'] = format_value if format_value else 'Unknown'

                # Limit length
                if len(resource['format']) > 50:
                    resource['format'] = resource['format'][:50]
                if not resource['format']:
                    resource['format'] = 'Unknown'

            # Validate and clean URL
            url = resource.get('url', '').strip()
            if url:
                resource['url'] = url

            # Keep all fields - don't delete anything
            # The validation should handle any issues, and we want to preserve all metadata
            # Note: We previously deleted resource_locator_function, hash_algorithm, conforms_to
            # but now we keep everything to avoid losing information

            # Ensure size is reasonable if present
            if 'size' in resource and resource['size']:
                try:
                    size = int(resource['size'])
                    if size < 0 or size > 10**12:  # Max 1TB
                        del resource['size']
                except (ValueError, TypeError):
                    del resource['size']

            # Clean description if present
            if 'description' in resource and resource['description']:
                desc = str(resource['description']).strip()
                if len(desc) > 1000:
                    desc = desc[:1000] + '...'
                resource['description'] = desc

            # Keep all valid resource fields for data.gov.gr
            # Based on the actual data.gov.gr structure
            valid_fields = {
                # Basic fields
                'url', 'name', 'format', 'description', 'size', 'mimetype',
                'resource_type', 'created', 'last_modified', 'rights', 'hash',

                # Access fields
                'access_url', 'download_url', 'access_services',

                # European standards fields
                'applicable_legislation', 'availability', 'license', 'language_options',

                # Translated fields
                'description_translated', 'name_translated',

                # QA and metadata fields
                'qa', 'archiver',

                # Other standard fields
                'state', 'position', 'package_id', 'id',
                'cache_url', 'cache_last_updated', 'datastore_active',
                'mimetype_inner', 'url_type', 'resource_id'
            }

            cleaned_resource = {}
            for field in resource:
                # Keep all valid fields (allow more flexibility)
                if field in valid_fields and resource[field] is not None:
                    cleaned_resource[field] = resource[field]
                # Also keep any other fields that don't cause validation issues
                elif field not in ['resource_locator_function', 'hash_algorithm', 'conforms_to']:
                    cleaned_resource[field] = resource[field]

            # Ensure translated fields for resource names and descriptions
            if 'name' in cleaned_resource:
                if 'name_translated' not in cleaned_resource:
                    # Create new translated field with both languages
                    cleaned_resource['name_translated'] = {
                        'en': cleaned_resource['name'],
                        'el': cleaned_resource['name']
                    }
                else:
                    # Update existing translated field to ensure both languages exist
                    name_trans = cleaned_resource['name_translated']
                    if not isinstance(name_trans, dict):
                        name_trans = {}
                    if not name_trans.get('en'):
                        name_trans['en'] = cleaned_resource['name']
                    if not name_trans.get('el'):
                        name_trans['el'] = name_trans.get('en', cleaned_resource['name'])
                    cleaned_resource['name_translated'] = name_trans

            if 'description' in cleaned_resource:
                desc = cleaned_resource.get('description', '')
                if 'description_translated' not in cleaned_resource:
                    # Create new translated field with both languages
                    cleaned_resource['description_translated'] = {
                        'en': desc,
                        'el': desc
                    }
                else:
                    # Update existing translated field to ensure both languages exist
                    desc_trans = cleaned_resource['description_translated']
                    if not isinstance(desc_trans, dict):
                        desc_trans = {}
                    if not desc_trans.get('en'):
                        desc_trans['en'] = desc
                    if not desc_trans.get('el'):
                        desc_trans['el'] = desc_trans.get('en', desc)
                    cleaned_resource['description_translated'] = desc_trans

            cleaned_resources.append(cleaned_resource)
            log.debug(f"Fixed resource: {cleaned_resource.get('name', 'unnamed')} - {cleaned_resource.get('format', 'unknown')}")

        # Replace resources with cleaned ones
        dataset_dict['resources'] = cleaned_resources
        log.info(f"Resource validation completed for dataset '{dataset_dict.get('name', 'unknown')}'. Kept {len(cleaned_resources)} valid resources.")

    def _preserve_resource_level_licenses(self, dataset_dict, source_data):
        """
        Preserve license information at resource level by inheriting from dataset level
        """
        if not dataset_dict.get('resources'):
            return

        # Collect all license information from dataset level
        dataset_licenses = {}

        if dataset_dict.get('license'):
            dataset_licenses['license'] = dataset_dict['license']
        if dataset_dict.get('license_id'):
            dataset_licenses['license_id'] = dataset_dict['license_id']
        if dataset_dict.get('license_title'):
            dataset_licenses['license_title'] = dataset_dict['license_title']
        if dataset_dict.get('license_url'):
            dataset_licenses['license_url'] = dataset_dict['license_url']

        # Also check if source data has license info that wasn't mapped to dataset
        if source_data:
            if source_data.get('license_id') and not dataset_dict.get('license_id'):
                dataset_licenses['license_id'] = source_data['license_id']
            if source_data.get('license_title') and not dataset_dict.get('license_title'):
                dataset_licenses['license_title'] = source_data['license_title']
            if source_data.get('license_url') and not dataset_dict.get('license_url'):
                dataset_licenses['license_url'] = source_data['license_url']

        # Apply dataset-level licenses to all resources that don't have their own licenses
        if dataset_licenses:
            resources_updated = 0
            for resource in dataset_dict.get('resources', []):
                resource_has_license = (
                    resource.get('license') or
                    resource.get('license_id') or
                    resource.get('license_title') or
                    resource.get('license_url')
                )

                if not resource_has_license:
                    # Inherit all available license fields from dataset
                    for field, value in dataset_licenses.items():
                        resource[field] = value
                        log.debug(f"Inherited {field} to resource: {resource.get('name', 'unnamed')} -> {value}")
                    resources_updated += 1

            if resources_updated > 0:
                log.info(f"Applied dataset-level licenses to {resources_updated} resources")

        return

    def _extract_and_preserve_contact_phone(self, dataset_dict, source_data):
        """
        Extract and preserve contact phone from source data
        """
        if not source_data:
            return

        # Try to extract phone from various possible locations in source data
        phone = None

        # Method 1: Direct contact_phone field
        if source_data.get('contact_phone'):
            phone = source_data['contact_phone']

        # Method 2: From contact_point array
        elif 'contact_point' in source_data and source_data['contact_point']:
            contact_point = source_data['contact_point']
            if isinstance(contact_point, list) and contact_point:
                for contact in contact_point:
                    if isinstance(contact, dict) and contact.get('hasTelephone'):
                        phone = contact['hasTelephone']
                        break
            elif isinstance(contact_point, dict) and contact_point.get('hasTelephone'):
                phone = contact_point['hasTelephone']

        # Method 3: From extras
        elif 'extras' in source_data:
            for extra in source_data['extras']:
                if extra.get('key') in ['contact_phone', 'hasTelephone', 'telephone']:
                    phone = extra.get('value')
                    break

        # Preserve the phone number if found
        if phone:
            dataset_dict['contact_phone'] = phone
            log.debug(f"Preserved contact phone: {phone}")

            # Also add to extras for backup
            extras = dataset_dict.setdefault('extras', [])
            existing_phone_extra = next((e for e in extras if e['key'] == 'contact_phone'), None)
            if not existing_phone_extra:
                extras.append({
                    'key': 'contact_phone',
                    'value': phone
                })
