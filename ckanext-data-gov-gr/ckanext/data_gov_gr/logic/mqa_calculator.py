# -*- coding: utf-8 -*-
"""
MQA (Metadata Quality Assessment) Calculator Module

This module provides functions to calculate MQA scores for datasets based on
various criteria defined in the MQA standard.
https://data.europa.eu/mqa/methodology?locale=en
"""

import logging
import json
import re
import requests
from typing import Dict, Any, List, Optional, Union, Tuple, Literal

log = logging.getLogger(__name__)

class MQACalculator:
    """
    Calculator for Metadata Quality Assessment (MQA) scores.

    This class provides methods to calculate scores for different dimensions
    of metadata quality:
    - Findability
    - Accessibility
    - Interoperability
    - Reusability
    - Contextuality
    """

    # Default timeout for URL accessibility checks (in seconds)
    DEFAULT_URL_CHECK_TIMEOUT = 2

    # Default vocabulary names
    DEFAULT_MACHINE_READABLE_FORMATS_VOCAB = 'Machine Readable File Format'
    DEFAULT_OPEN_FORMATS_VOCAB = 'File Type - Non Proprietary Format'
    DEFAULT_LICENSES_VOCAB = 'Licence'
    DEFAULT_ACCESS_RIGHTS_VOCAB = 'Access right'
    DEFAULT_MEDIA_TYPES_VOCAB = 'Media types'

    # Fallback vocabularies in case the dynamic loading fails
    _FALLBACK_MACHINE_READABLE_FORMATS = [
        'csv', 'json', 'xml', 'rdf', 'xlsx', 'xls', 
        'ods', 'shp', 'kml', 'geojson', 'jsonld', 'turtle',
        'n3', 'ntriples', 'nquads', 'trig', 'trix'
    ]

    _FALLBACK_OPEN_FORMATS = [
        'csv', 'json', 'xml', 'rdf', 'ods', 'odf', 
        'txt', 'html', 'pdf/a', 'jsonld', 'turtle',
        'n3', 'ntriples', 'nquads', 'trig', 'trix'
    ]

    _FALLBACK_KNOWN_LICENSES = [
        'cc-by', 'cc-by-sa', 'cc-zero', 'odc-by', 
        'odc-odbl', 'public-domain', 'cc-by-nc', 'cc-by-nc-sa',
        'cc-by-nd', 'cc-by-nc-nd', 'gfdl', 'ogl', 'notspecified'
    ]

    _FALLBACK_KNOWN_ACCESS_RIGHTS = [
        'public', 'private', 'restricted', 'non-public', 'sensitive'
    ]

    _FALLBACK_IANA_MEDIA_TYPES = [
        'application/json', 'application/xml', 'text/csv', 'text/html',
        'application/pdf', 'application/zip', 'image/jpeg', 'image/png',
        'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/rdf+xml', 'text/turtle', 'application/ld+json',
        'application/geo+json', 'application/vnd.google-earth.kml+xml'
    ]

    def __init__(self, check_urls=True, url_check_timeout=None, 
                 machine_readable_formats_vocab=None, open_formats_vocab=None, 
                 licenses_vocab=None, access_rights_vocab=None, media_types_vocab=None):
        """
        Initialize the MQA calculator.

        Args:
            check_urls: Whether to check URL accessibility (default: True)
            url_check_timeout: Timeout for URL accessibility checks in seconds (default: DEFAULT_URL_CHECK_TIMEOUT)
            machine_readable_formats_vocab: Name of the vocabulary for machine-readable formats (default: DEFAULT_MACHINE_READABLE_FORMATS_VOCAB)
            open_formats_vocab: Name of the vocabulary for open formats (default: DEFAULT_OPEN_FORMATS_VOCAB)
            licenses_vocab: Name of the vocabulary for licenses (default: DEFAULT_LICENSES_VOCAB)
            access_rights_vocab: Name of the vocabulary for access rights (default: DEFAULT_ACCESS_RIGHTS_VOCAB)
            media_types_vocab: Name of the vocabulary for media types (default: DEFAULT_MEDIA_TYPES_VOCAB)
        """
        self.check_urls = check_urls
        self.url_check_timeout = url_check_timeout or self.DEFAULT_URL_CHECK_TIMEOUT
        self._url_cache = {}  # Cache for URL accessibility checks
        self._status_code_cache = {}  # Cache for URL status codes
        self._access_urls = []  # List of access URLs
        self._download_urls = []  # List of download URLs
        self._resources = []  # List of resources in the current dataset

        # Set vocabulary names
        self.machine_readable_formats_vocab = machine_readable_formats_vocab or self.DEFAULT_MACHINE_READABLE_FORMATS_VOCAB
        self.open_formats_vocab = open_formats_vocab or self.DEFAULT_OPEN_FORMATS_VOCAB
        self.licenses_vocab = licenses_vocab or self.DEFAULT_LICENSES_VOCAB
        self.access_rights_vocab = access_rights_vocab or self.DEFAULT_ACCESS_RIGHTS_VOCAB
        self.media_types_vocab = media_types_vocab or self.DEFAULT_MEDIA_TYPES_VOCAB

        # Initialize vocabularies with fallback values
        self.machine_readable_formats = self._FALLBACK_MACHINE_READABLE_FORMATS
        self.open_formats = self._FALLBACK_OPEN_FORMATS
        self.known_licenses = set(self._FALLBACK_KNOWN_LICENSES)
        self.known_access_rights = self._FALLBACK_KNOWN_ACCESS_RIGHTS
        self.iana_media_types = self._FALLBACK_IANA_MEDIA_TYPES

        # Load vocabularies from the vocabulary admin extension
        self._load_vocabularies()

    def _load_vocabularies(self):
        """
        Load vocabularies from the vocabulary admin extension.

        If a vocabulary cannot be loaded, the fallback values will be used.
        """
        try:
            import ckan.plugins.toolkit as toolkit

            # Load machine-readable formats
            try:
                vocab_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                    {}, {'id': self.machine_readable_formats_vocab}
                )
                if vocab_data and 'tags' in vocab_data:
                    self.machine_readable_formats = [tag['name'].lower() for tag in vocab_data['tags']]
            except (toolkit.ObjectNotFound, toolkit.ValidationError, Exception) as e:
                log.warning(f"Could not load machine-readable formats vocabulary: {e}")

            # Load open formats
            try:
                vocab_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                    {}, {'id': self.open_formats_vocab}
                )
                if vocab_data and 'tags' in vocab_data:
                    self.open_formats = [tag['name'].lower() for tag in vocab_data['tags']]

            except (toolkit.ObjectNotFound, toolkit.ValidationError, Exception) as e:
                log.warning(f"Could not load open formats vocabulary: {e}")

            # Load licenses
            try:
                vocab_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                    {}, {'id': self.licenses_vocab}
                )
                if vocab_data and 'tags' in vocab_data:
                    for tag in vocab_data['tags']:
                        self._add_license_tag(tag)

            except (toolkit.ObjectNotFound, toolkit.ValidationError, Exception) as e:
                log.warning(f"Could not load licenses vocabulary: {e}")

            # Also load CKAN core license registry
            self._load_known_licenses_from_ckan(toolkit)

            # Load access rights
            try:
                vocab_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                    {}, {'id': self.access_rights_vocab}
                )
                if vocab_data and 'tags' in vocab_data:
                    self.known_access_rights = [tag['name'].lower() for tag in vocab_data['tags']]

            except (toolkit.ObjectNotFound, toolkit.ValidationError, Exception) as e:
                log.warning(f"Could not load access rights vocabulary: {e}")

            # Load media types
            try:
                vocab_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                    {}, {'id': self.media_types_vocab}
                )
                if vocab_data and 'tags' in vocab_data:
                    self.iana_media_types = [tag['name'].lower() for tag in vocab_data['tags']]
            except (toolkit.ObjectNotFound, toolkit.ValidationError, Exception) as e:
                log.warning(f"Could not load media types vocabulary: {e}")

        except ImportError:
            log.warning("Could not import toolkit, using fallback vocabularies")
        except Exception as e:
            log.exception(f"Error loading vocabularies: {e}")

    def _is_bulk_download_resource(self, resource: Dict[str, Any]) -> bool:
        """
        Check if a resource is a bulk download resource created by DownloadAll extension.

        Args:
            resource: The resource dictionary

        Returns:
            True if the resource is a bulk download resource, False otherwise
        """
        return bool(resource.get('downloadall_metadata_modified'))

    def _filter_bulk_download_resources(self, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter out bulk download resources from a list of resources.

        Args:
            resources: List of resource dictionaries

        Returns:
            List of resources without bulk download resources
        """
        return [resource for resource in resources if not self._is_bulk_download_resource(resource)]

    def calculate_all_scores(self, dataset_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate all MQA scores for a dataset.

        Args:
            dataset_dict: The dataset dictionary

        Returns:
            A dictionary containing all MQA scores
        """
        # Store the resources for this dataset, filtering out bulk download resources
        all_resources = dataset_dict.get('resources', [])
        self._resources = self._filter_bulk_download_resources(all_resources)

        scores = {
            'findability': self.calculate_findability_score(dataset_dict),
            'accessibility': self.calculate_accessibility_score(dataset_dict),
            'interoperability': self.calculate_interoperability_score(dataset_dict),
            'reusability': self.calculate_reusability_score(dataset_dict),
            'contextuality': self.calculate_contextuality_score(dataset_dict)
        }

        # Calculate distribution quality scores
        distribution_scores = self.calculate_distribution_quality_scores(dataset_dict)
        scores['distributions'] = distribution_scores

        # Calculate average distribution percentage
        avg_distribution_percentage = 0
        if distribution_scores:
            distribution_percentages = [dist['percentage'] for dist in distribution_scores]
            avg_distribution_percentage = sum(distribution_percentages) / len(distribution_percentages)
            scores['avg_distribution_percentage'] = round(avg_distribution_percentage, 1)

        # Calculate total score and percentage
        # Sum up the dimension scores without additional weighting
        total_score = scores['findability'] + scores['accessibility'] + scores['interoperability'] + scores['reusability'] + scores['contextuality']
        max_score = 100 + 100 + 110 + 75 + 20  # 405 total possible points
        percentage = round((total_score / max_score) * 100, 1)

        # Round all scores to 1 decimal place for consistent display
        scores['findability'] = round(scores['findability'], 1)
        scores['accessibility'] = round(scores['accessibility'], 1)
        scores['interoperability'] = round(scores['interoperability'], 1)
        scores['reusability'] = round(scores['reusability'], 1)
        scores['contextuality'] = round(scores['contextuality'], 1)

        scores['total'] = round(total_score, 1)
        scores['max_score'] = max_score
        scores['percentage'] = percentage

        return scores

    def calculate_findability_score(self, dataset_dict: Dict[str, Any]) -> float:
        """
        Calculate the Findability score (max 100 points).

        Since findability is based on dataset-level metadata, not resource-level metadata,
        we'll calculate the score directly from the dataset properties.

        Criteria:
        - Keywords (dcat:keyword) - 30 points
        - Categories (dcat:theme) - 30 points
        - Spatial information (dct:spatial) - 20 points
        - Temporal information (dct:temporal) - 20 points
        """
        resources = dataset_dict.get('resources', [])
        if not resources:
            return 0.0

        # For dataset-level criteria, we use binary values (0 or 1) instead of percentages
        has_keywords = 1 if dataset_dict.get('tags') and len(dataset_dict.get('tags', [])) > 0 else 0

        has_categories = 1 if (dataset_dict.get('groups') and len(dataset_dict.get('groups', [])) > 0) or dataset_dict.get('theme') else 0

        has_spatial = 1 if dataset_dict.get('spatial') or dataset_dict.get('spatial_uri') or dataset_dict.get('spatial_coverage') else 0

        has_temporal = 1 if dataset_dict.get('temporal_start') or dataset_dict.get('temporal_end') or dataset_dict.get('temporal') or dataset_dict.get('temporal_coverage') else 0

        # Calculate score as (binary value * criterion weight) for each criterion
        score = (
            has_keywords * 30 +
            has_categories * 30 +
            has_spatial * 20 +
            has_temporal * 20
        )

        return score  # float from 0.0 to 100.0

    def _check_url_accessibility(self, url: str) -> bool:
        """
        Check if a URL is accessible by using the archiver plugin's Archival model if available,
        or falling back to making an HTTP HEAD request.

        Args:
            url: The URL to check

        Returns:
            True if the URL is accessible, False otherwise
        """
        if not url:
            return False

        # If URL checking is disabled, assume all URLs are accessible
        if not self.check_urls:
            return True

        log.debug(f"Checking accessibility for {url!r}")

        # First, try to use the archiver plugin's Archival model if it's available
        try:
            from ckanext.archiver.model import Archival, Status

            # Check if the URL is in our cache
            if url in self._url_cache:
                return self._url_cache[url]

            # Try to find the resource ID for this URL
            resource_id = None
            for resource in self._resources:
                if resource.get('url') == url or resource.get('download_url') == url:
                    resource_id = resource.get('id')
                    break

            # If we found a resource ID, check if it has been archived
            if resource_id:
                archival = Archival.get_for_resource(resource_id)
                if archival:
                    # If the resource has been archived, use the archival status
                    is_accessible = Status.is_ok(archival.status_id)
                    log.debug(f"→ Using archiver status: {archival.status} (accessible: {is_accessible})")

                    # Cache the result
                    self._url_cache[url] = is_accessible

                    # Store status code or message for analytics
                    if archival.reason:
                        self._status_code_cache[url] = archival.reason

                    return is_accessible
        except ImportError:
            log.debug("Archiver plugin not available, falling back to direct URL check")
        except Exception as e:
            log.warning(f"Error using archiver plugin: {e}, falling back to direct URL check")

        # Fall back to the original method if archiver is not available or if the resource hasn't been archived
        try:
            # Use a timeout to avoid hanging if the server doesn't respond
            response = requests.head(url, timeout=self.url_check_timeout, allow_redirects=True)
            # Consider 2xx and 3xx status codes as accessible
            result = 200 <= response.status_code < 400
            log.debug(f"→ status_code = {response.status_code}")
            self._status_code_cache[url] = response.status_code

            # Cache the result
            self._url_cache[url] = result

            return result
        except requests.RequestException as e:
            log.debug(f"→ exception: {e}")
            # Store error code or message for analytics
            if hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'status_code'):
                self._status_code_cache[url] = e.response.status_code
                # Ensure 404 responses are correctly classified as inaccessible
                if e.response.status_code == 404:
                    log.debug(f"→ 404 Not Found, marking as inaccessible")
            else:
                self._status_code_cache[url] = str(e)

            # Cache the result
            self._url_cache[url] = False

            return False

    def calculate_accessibility_score(self, dataset_dict: Dict[str, Any]) -> float:
        """
        Calculate the Accessibility score (max 100 points).

        For each sub-criterion:
        1. Calculate the percentage of resources that satisfy it
        2. Multiply by the points allocated for that sub-criterion
        3. Sum up all sub-criteria scores

        Criteria:
        - Access URL accessibility (dcat:accessURL) - 50 points
        - Download URL existence (dcat:downloadURL) - 20 points
        - Download URL accessibility (dcat:downloadURL) - 30 points
        """
        resources = self._filter_bulk_download_resources(dataset_dict.get('resources', []))
        n = len(resources)
        if n == 0:
            return 0.0

        # 1) Count how many resources satisfy each sub-criterion
        access_url_accessible_count = sum(1 for r in resources if r.get('url') and
                                         self._check_url_accessibility(r.get('url')))
        # Only check if download_url exists
        download_url_exists_count = sum(1 for r in resources if r.get('download_url'))
        download_url_accessible_count = sum(1 for r in resources if r.get('download_url') and
                                           self._check_url_accessibility(r.get('download_url')))

        # 2) Calculate score as (prevalence * sub-criterion weight) for each sub-criterion
        score = (
            (access_url_accessible_count / n) * 50 +
            (download_url_exists_count / n) * 20 +
            (download_url_accessible_count / n) * 30
        )

        return score  # float from 0.0 to 100.0

    def _is_format_in_vocabulary(self, format_str: str) -> bool:
        """
        Check if a format is in a controlled vocabulary.

        Args:
            format_str: The format string to check

        Returns:
            True if the format is in a controlled vocabulary, False otherwise
        """
        if not format_str:
            return False

        # Convert to lowercase for case-insensitive comparison
        format_lower = format_str.lower()

        # Check if the format is in our known formats list
        return format_lower in self.machine_readable_formats or format_lower in self.open_formats

    def _is_mimetype_in_vocabulary(self, mimetype: str) -> bool:
        """
        Check if a media type is in the IANA media types list.

        Args:
            mimetype: The media type to check

        Returns:
            True if the media type is in the IANA media types list, False otherwise
        """
        if not mimetype:
            return False

        # Convert to lowercase for case-insensitive comparison
        mimetype_lower = mimetype.lower()

        # Check if the media type is in our IANA media types list
        return any(mt.lower() == mimetype_lower for mt in self.iana_media_types)

    def _extract_access_right_id_from_uri(self, access_right_uri: str) -> str:
        """
        Extract the access right ID from an access right URI.

        For example, from "http://publications.europa.eu/resource/authority/access-right/PUBLIC"
        it extracts "public".

        Args:
            access_right_uri: The access right URI

        Returns:
            The access right ID extracted from the URI, or the original URI if no ID can be extracted
        """
        if not access_right_uri:
            return ""

        log.debug(f"Extracting access right ID from URI: '{access_right_uri}'")

        # Try to extract the access right ID from the URI
        # Common patterns include:
        # - http://publications.europa.eu/resource/authority/access-right/PUBLIC

        # Try to extract from publications.europa.eu URIs
        if 'publications.europa.eu/resource/authority/access-right/' in access_right_uri.lower():
            parts = access_right_uri.split('/')
            if parts and len(parts) > 0:
                result = parts[-1].lower()
                log.debug(f"Extracted access right ID from publications.europa.eu URI: '{result}'")
                return result

        # Return the original URI lowercased if no pattern matches
        result = access_right_uri.lower()
        log.debug(f"No pattern matched, returning lowercased URI: '{result}'")
        return result

    def is_access_right_in_vocabulary(self, access_right: str) -> bool:
        """
        Check if an access right value is in the access rights vocabulary.

        Args:
            access_right: The access right value to check

        Returns:
            True if the access right is in the vocabulary, False otherwise
        """
        if not access_right:
            return False

        # Convert to lowercase for case-insensitive comparison
        access_right_lower = access_right.lower()
        log.debug(f"Checking if access right '{access_right}' is in vocabulary")
        log.debug(f"Access right lowercase: '{access_right_lower}'")
        log.debug(f"Known access rights: {self.known_access_rights}")

        # Try to extract the access right ID from the URI
        access_right_id = self._extract_access_right_id_from_uri(access_right)
        log.debug(f"Extracted access right ID: '{access_right_id}'")

        # Check if the access right or the extracted ID is in the known access rights list
        result = access_right_lower in self.known_access_rights or access_right_id in self.known_access_rights
        log.debug(f"Access right '{access_right}' is in vocabulary: {result}")
        return result

    def has_valid_contact_point(self, dataset_dict: Dict[str, Any]) -> bool:
        """
        Check if a dataset has a valid contact point.

        A valid contact point is one where at least one of the following is true:
        1. The dataset has a non-empty maintainer field
        2. The dataset has a non-empty maintainer_email field
        3. The dataset has a non-empty contact_email field
        4. The dataset has a contact_point field with at least one non-empty value
           (email, name, uri, or url)
        5. The dataset has a contact field with at least one non-empty value
           (email, name, uri, or url)

        Args:
            dataset_dict: The dataset dictionary

        Returns:
            True if the dataset has a valid contact point, False otherwise
        """
        # Check for maintainer, maintainer_email, or contact_email
        if dataset_dict.get('maintainer') or dataset_dict.get('maintainer_email') or dataset_dict.get('contact_email'):
            return True

        # Check for contact_point with non-empty values
        contact_point = dataset_dict.get('contact_point')
        if contact_point:
            try:
                # If contact_point is already a list or dict, use it directly
                if isinstance(contact_point, list):
                    contacts = contact_point
                elif isinstance(contact_point, dict):
                    contacts = [contact_point]
                else:
                    # Otherwise, try to parse it as JSON
                    contacts = json.loads(contact_point)
                    if not isinstance(contacts, list):
                        contacts = [contacts]

                # Check if any contact has non-empty values
                for contact in contacts:
                    if isinstance(contact, dict) and any(contact.get(field) for field in ['email', 'name', 'uri', 'url']):
                        return True
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                log.warning(f"Error parsing contact_point: {e}")
                # If we can't parse it, assume it's not valid
                return False

        # Check for contact with non-empty values
        contact = dataset_dict.get('contact')
        if contact:
            try:
                # If contact is already a list or dict, use it directly
                if isinstance(contact, list):
                    contacts = contact
                elif isinstance(contact, dict):
                    contacts = [contact]
                else:
                    # Otherwise, try to parse it as JSON
                    contacts = json.loads(contact)
                    if not isinstance(contacts, list):
                        contacts = [contacts]

                # Check if any contact has non-empty values
                for contact_item in contacts:
                    if isinstance(contact_item, dict) and any(contact_item.get(field) for field in ['email', 'name', 'uri', 'url']):
                        return True
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                log.warning(f"Error parsing contact: {e}")
                # If we can't parse it, assume it's not valid
                return False

        return False

    def _check_dcat_ap_compliance(self, dataset_dict: Dict[str, Any]) -> bool:
        """
        Check if a dataset is compliant with DCAT-AP.

        This method implements a simplified DCAT-AP compliance check that only verifies
        the presence of title and description, and if resources exist, checks if they have an access URL.


        Args:
            dataset_dict: The dataset dictionary

        Returns:
            True if the dataset is compliant with the simplified DCAT-AP check, False otherwise
        """
        # Check for title and description (required)
        has_title = bool(dataset_dict.get('title'))
        has_description = bool(dataset_dict.get('description') or dataset_dict.get('notes'))

        # If there are no resources, we only check title and description
        resources = dataset_dict.get('resources', [])
        if not resources:
            return has_title and has_description

        # If there are resources, check if at least one has an access URL
        has_access_url = any(bool(r.get('url')) for r in resources)

        return has_title and has_description and has_access_url

    def calculate_interoperability_score(self, dataset_dict: Dict[str, Any]) -> float:
        """
        Calculate the Interoperability score (max 110 points).

        For each sub-criterion:
        1. Calculate the percentage of resources that satisfy it
        2. Multiply by the points allocated for that sub-criterion
        3. Sum up all sub-criteria scores

        Criteria:
        - Format specified (dct:format) - 20 points
        - Media type specified (dcat:mediaType) - 10 points
        - Format/media type from vocabulary - 10 points
        - Open format - 20 points
        - Machine-readable format - 20 points
        - DCAT-AP compliance - 30 points (dataset-level)
        """
        resources = self._filter_bulk_download_resources(dataset_dict.get('resources', []))
        n = len(resources)
        if n == 0:
            return 0.0

        # 1) Count how many resources satisfy each sub-criterion
        format_specified_count = sum(1 for r in resources if r.get('format'))
        media_type_specified_count = sum(1 for r in resources if r.get('mimetype'))
        format_from_vocab_count = sum(1 for r in resources if r.get('format') and self._is_format_in_vocabulary(r.get('format')))
        media_type_from_vocab_count = sum(1 for r in resources if r.get('mimetype') and self._is_mimetype_in_vocabulary(r.get('mimetype')))
        open_format_count = sum(1 for r in resources if r.get('format', '').lower() in self.open_formats)
        machine_readable_count = sum(1 for r in resources if r.get('format', '').lower() in self.machine_readable_formats)

        # For format/media type from vocabulary, we'll use the higher of the two counts
        format_vocab_count = max(format_from_vocab_count, media_type_from_vocab_count)

        # Check for DCAT-AP compliance (dataset-level)
        dcat_compliance = 1 if self._check_dcat_ap_compliance(dataset_dict) else 0

        # 2) Calculate score as (prevalence * sub-criterion weight) for each sub-criterion
        score = (
            (format_specified_count / n) * 20 +
            (media_type_specified_count / n) * 10 +
            (format_vocab_count / n) * 10 +
            (open_format_count / n) * 20 +
            (machine_readable_count / n) * 20 +
            dcat_compliance * 30  # Dataset-level criterion
        )

        return score  # float from 0.0 to 110.0

    def _extract_license_id_from_uri(self, license_uri: str) -> str:
        """
        Extract the license ID from a license URI.

        For example, from "http://publications.europa.eu/resource/authority/licence/AGPL_3_0"
        it extracts "agpl_3_0".

        Args:
            license_uri: The license URI

        Returns:
            The license ID extracted from the URI, or the original URI if no ID can be extracted
        """
        if not license_uri:
            return ""

        # Try to extract the license ID from the URI
        # Common patterns include:
        # - http://publications.europa.eu/resource/authority/licence/AGPL_3_0
        # - http://creativecommons.org/licenses/by/4.0/

        # First, try to extract from publications.europa.eu URIs
        if 'publications.europa.eu/resource/authority/licence/' in license_uri.lower():
            parts = license_uri.split('/')
            if parts and len(parts) > 0:
                return parts[-1].lower()

        # Try to extract from creativecommons.org URIs
        if 'creativecommons.org/licenses/' in license_uri.lower():
            # Extract the license type (e.g., by, by-sa, zero)
            match = re.search(r'creativecommons\.org/licenses/([^/]+)', license_uri.lower())
            if match:
                license_type = match.group(1)
                if license_type == 'zero':
                    return 'cc-zero'
                return f'cc-{license_type}'

        # Return the original URI lowercased if no pattern matches
        return license_uri.lower()

    def _get_resource_license_url(self, resource: Dict[str, Any]) -> str:
        """
        Return the license URL defined on a resource (empty string if not present).
        """
        value = resource.get('license_url')
        if value:
            value_str = str(value).strip()
            if value_str:
                return value_str
        return ''

    def _resource_has_license(self, resource: Dict[str, Any]) -> bool:
        return bool(self._get_resource_license_url(resource))

    def _resource_license_matches_vocabulary(self, resource: Dict[str, Any]) -> bool:
        license_url = self._get_resource_license_url(resource)
        return bool(license_url and self._license_in_registry(license_url))

    def _add_known_license(self, value: Optional[str]) -> None:
        if not value:
            return
        normalized = str(value).strip().lower()
        if not normalized:
            return

        # Store common variations to improve matching robustness
        variants = {
            normalized,
            normalized.rstrip('/'),
            normalized.replace('_', '-'),
            normalized.replace('-', '_'),
        }

        for variant in variants:
            if variant:
                self.known_licenses.add(variant)

    def _add_license_tag(self, tag: Dict[str, Any]) -> None:
        """
        Add all useful values from a vocabulary tag entry to the known license set.
        """
        if not isinstance(tag, dict):
            return

        for field in ('name', 'value', 'value_uri', 'uri', 'display_name'):
            self._add_known_license(tag.get(field))

        extras = tag.get('extras')
        if isinstance(extras, (list, tuple)):
            for extra in extras:
                if isinstance(extra, dict):
                    self._add_known_license(extra.get('value'))
                    self._add_known_license(extra.get('value_uri'))
                    self._add_known_license(extra.get('uri'))

    def _load_known_licenses_from_ckan(self, toolkit) -> None:
        """
        Load licenses defined in CKAN's core registry and merge them with known licenses.
        """
        try:
            license_list = toolkit.get_action('license_list')({}, {'all_fields': True})
            for item in license_list:
                for field in ('id', 'title', 'url'):
                    self._add_known_license(item.get(field))
        except Exception as e:
            log.warning(f"Could not load license registry: {e}")

    def _generate_license_candidates(self, license_value: str) -> set:
        """
        Generate normalized variants of a license value for matching.
        """
        if not license_value:
            return set()

        normalized = license_value.strip().lower()
        candidates = {
            normalized,
            normalized.rstrip('/'),
            normalized.replace('_', '-'),
            normalized.replace('-', '_'),
        }

        extracted = self._extract_license_id_from_uri(normalized)
        if extracted:
            candidates.add(extracted)

        # Add variants without version suffixes like "-4.0" or "_3_0"
        for candidate in list(candidates):
            if candidate:
                simplified = re.sub(r'[-_/ ]?v?\d+(?:\.\d+)*$', '', candidate)
                if simplified:
                    candidates.add(simplified)

        return {candidate for candidate in candidates if candidate}

    def _license_in_registry(self, license_value: str) -> bool:
        """
        Check if a license value matches any known license entry.
        """
        if not license_value:
            return False

        candidates = self._generate_license_candidates(license_value)
        return any(candidate in self.known_licenses for candidate in candidates)

    def calculate_reusability_score(self, dataset_dict: Dict[str, Any]) -> float:
        """
        Calculate the Reusability score (max 75 points).

        Reusability is mostly based on dataset-level metadata, but license information
        is also checked at the resource level.

        Criteria:
        - License information (dct:license) - 20 points (resource-level)
        - License vocabulary (dct:license) - 10 points (resource-level)
        - Access rights (dct:accessRights) - 10 points (dataset-level)
        - Access rights vocabulary (dct:accessRights) - 5 points (dataset-level)
        - Contact point (dcat:contactPoint) - 20 points (dataset-level)
        - Publisher (dct:publisher) - 10 points (dataset-level)
        """
        resources = self._filter_bulk_download_resources(dataset_dict.get('resources', []))
        n = len(resources)
        if n == 0:
            return 0.0

        # Check license information at the resource level
        license_count = sum(1 for r in resources if self._resource_has_license(r))
        license_score = (license_count / n) * 20

        # Check license vocabulary at the resource level
        license_vocab_count = sum(
            1
            for r in resources
            if self._resource_license_matches_vocabulary(r)
        )
        license_vocab_score = (license_vocab_count / n) * 10

        # For dataset-level criteria, we use binary values (0 or 1) instead of percentages
        has_access_rights = 1 if dataset_dict.get('access_rights') else 0

        access_rights = dataset_dict.get('access_rights', '') if dataset_dict.get('access_rights') else ''
        access_rights_in_vocab = 1 if self.is_access_right_in_vocabulary(access_rights) else 0

        has_contact_point = 1 if self.has_valid_contact_point(dataset_dict) else 0

        has_publisher = 1 if dataset_dict.get('organization') or dataset_dict.get('publisher') else 0

        # Calculate score as (binary value * criterion weight) for each criterion
        # For license information and license vocabulary, use the resource-level scores
        score = (
            license_score +
            license_vocab_score +
            has_access_rights * 10 +
            access_rights_in_vocab * 5 +
            has_contact_point * 20 +
            has_publisher * 10
        )

        return score  # float from 0.0 to 75.0

    def calculate_contextuality_score(self, dataset_dict: Dict[str, Any]) -> float:
        """
        Calculate the Contextuality score (max 20 points).

        For each sub-criterion:
        1. Calculate the percentage of resources that satisfy it
        2. Multiply by the points allocated for that sub-criterion
        3. Sum up all sub-criteria scores

        Criteria:
        - Rights (dct:rights) - 5 points
        - File size (dcat:byteSize) - 5 points
        - Issue date (dct:issued) - 5 points
        - Modification date (dct:modified) - 5 points
        """
        resources = self._filter_bulk_download_resources(dataset_dict.get('resources', []))
        n = len(resources)
        if n == 0:
            return 0.0

        # 1) Count how many resources satisfy each sub-criterion
        rights_count = sum(1 for r in resources if r.get('rights'))
        size_count = sum(1 for r in resources if r.get('size'))
        issue_count = sum(1 for r in resources if r.get('created'))
        mod_count = sum(1 for r in resources if r.get('metadata_modified') )

        # 2) Calculate score as (prevalence * sub-criterion weight) for each sub-criterion
        score = (
            (rights_count / n) * 5 +
            (size_count / n) * 5 +
            (issue_count / n) * 5 +
            (mod_count / n) * 5
        )

        return score  # float from 0.0 to 20.0

    def get_most_frequent_access_url_status_codes(self, limit: int = 3) -> List[Tuple]:
        """
        Get the most frequent access URL status codes.

        Args:
            limit: Maximum number of status codes to return (default: 3)

        Returns:
            A list of tuples (status_code, count) sorted by count in descending order
        """
        # Count status codes for access URLs
        status_code_counts = {}
        for url in self._access_urls:
            if url in self._status_code_cache:
                status_code = self._status_code_cache[url]
                status_code_counts[status_code] = status_code_counts.get(status_code, 0) + 1

        # Sort by count in descending order
        sorted_counts = sorted(status_code_counts.items(), key=lambda x: x[1], reverse=True)

        # Return the top N status codes
        return sorted_counts[:limit]

    def get_most_frequent_download_url_status_codes(self, limit: int = 3) -> List[Tuple]:
        """
        Get the most frequent download URL status codes.

        Args:
            limit: Maximum number of status codes to return (default: 3)

        Returns:
            A list of tuples (status_code, count) sorted by count in descending order
        """
        # Count status codes for download URLs
        status_code_counts = {}
        for url in self._download_urls:
            if url in self._status_code_cache:
                status_code = self._status_code_cache[url]
                status_code_counts[status_code] = status_code_counts.get(status_code, 0) + 1

        # Sort by count in descending order
        sorted_counts = sorted(status_code_counts.items(), key=lambda x: x[1], reverse=True)

        # Return the top N status codes
        return sorted_counts[:limit]

    def calculate_distribution_quality_scores(self, dataset_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Calculate quality scores for individual distributions (resources).

        Evaluates each resource based on:
        - Accessibility (access URL, download URL)
        - Format quality (open format, machine-readable)
        - Metadata completeness (title, description, license, etc.)
        - Documentation (rights, size, dates)

        Args:
            dataset_dict: The dataset dictionary

        Returns:
            A list of dictionaries containing quality scores for each distribution
        """
        distribution_scores = []
        resources = self._filter_bulk_download_resources(dataset_dict.get('resources', []))

        # Calculate dataset-level criteria satisfaction
        dataset_criteria = self._calculate_dataset_criteria_satisfaction(resources)

        for resource in resources:
            # Skip bulk download resources in distribution scores
            if self._is_bulk_download_resource(resource):
                continue

            # Get criteria satisfaction for this resource
            criteria = self._calculate_resource_criteria_satisfaction(resource)

            # Calculate scores based on criteria satisfaction
            scores = {
                'accessibility': self._calculate_distribution_accessibility(resource),
                'format_quality': self._calculate_distribution_format_quality(resource),
                'metadata_completeness': self._calculate_distribution_metadata_completeness(resource),
                'documentation': self._calculate_distribution_documentation(resource)
            }

            # Calculate total score and percentage
            total_score = sum(scores.values())
            max_score = 100 + 50 + 30 + 20  # 200 total possible points
            percentage = round((total_score / max_score) * 100, 1)

            # Round all scores to 1 decimal place for consistent display
            scores['accessibility'] = round(scores['accessibility'], 1)
            scores['format_quality'] = round(scores['format_quality'], 1)
            scores['metadata_completeness'] = round(scores['metadata_completeness'], 1)
            scores['documentation'] = round(scores['documentation'], 1)

            scores['total'] = round(total_score, 1)
            scores['max_score'] = max_score
            scores['percentage'] = percentage

            # Add criteria satisfaction to scores
            scores['criteria'] = criteria

            # Add resource metadata for display
            scores['resource_id'] = resource.get('id', '')
            scores['name'] = resource.get('name', resource.get('name_translated', {}).get('en', ''))
            scores['format'] = resource.get('format', '')

            distribution_scores.append(scores)

        # Add dataset-level criteria satisfaction to the first distribution
        if distribution_scores:
            distribution_scores[0]['dataset_criteria'] = dataset_criteria

            # Add most frequent access URL status codes and download URL status codes
            distribution_scores[0]['most_frequent_access_url_status_codes'] = self.get_most_frequent_access_url_status_codes()
            distribution_scores[0]['most_frequent_download_url_status_codes'] = self.get_most_frequent_download_url_status_codes()

        return distribution_scores

    def _calculate_distribution_accessibility(self, resource: Dict[str, Any]) -> int:
        """
        Calculate the accessibility score for a distribution (max 100 points).

        Criteria:
        - AccessURL accessibility - 50 points
        - DownloadURL existence - 20 points
        - DownloadURL accessibility - 30 points

        Args:
            resource: The resource dictionary

        Returns:
            The accessibility score (0-100)
        """
        score = 0

        # Check for access URL
        access_url = resource.get('url')
        if access_url:
            # Store access URL for analysis
            if access_url not in self._access_urls:
                self._access_urls.append(access_url)

            # Check if access URL is accessible
            if self._check_url_accessibility(access_url):
                score += 50

        # Check for download URL
        download_url = resource.get('download_url')

        # Check if download_url exists
        if download_url:
            # Store download URL for analysis
            if download_url not in self._download_urls:
                self._download_urls.append(download_url)

            score += 20  # DownloadURL existence
            if self._check_url_accessibility(download_url):
                score += 30  # DownloadURL accessibility

        return score

    def _calculate_distribution_format_quality(self, resource: Dict[str, Any]) -> int:
        """
        Calculate the format quality score for a distribution (max 50 points).

        Criteria:
        - Format specified - 10 points
        - Media type specified - 10 points
        - Open format - 15 points
        - Machine-readable format - 15 points

        Args:
            resource: The resource dictionary

        Returns:
            The format quality score (0-50)
        """
        score = 0

        # Check for format
        if resource.get('format'):
            score += 10

            # Check if format is open
            if resource.get('format', '').lower() in self.open_formats:
                score += 15

            # Check if format is machine-readable
            if resource.get('format', '').lower() in self.machine_readable_formats:
                score += 15

        # Check for media type
        if resource.get('mimetype'):
            score += 10

        return score

    def _calculate_distribution_metadata_completeness(self, resource: Dict[str, Any]) -> int:
        """
        Calculate the metadata completeness score for a distribution (max 30 points).

        Criteria:
        - License present (dct:license/license_url) - 20 points
        - License from controlled vocabulary - 10 points

        Args:
            resource: The resource dictionary

        Returns:
            The metadata completeness score (0-30)
        """
        score = 0

        # Check for license (via license_url)
        license_url = self._get_resource_license_url(resource)
        if license_url:
            score += 20
            log.debug(f"Resource license URL: {license_url}")
            if self._license_in_registry(license_url):
                log.debug("License found in known licenses list")
                score += 10
            else:
                log.debug("License not found in known licenses list")

        return score

    def _calculate_distribution_documentation(self, resource: Dict[str, Any]) -> int:
        """
        Calculate the documentation score for a distribution (max 20 points).

        Criteria:
        - Rights information - 5 points
        - File size - 5 points
        - Issue date - 5 points
        - Modification date - 5 points

        Args:
            resource: The resource dictionary

        Returns:
            The documentation score (0-20)
        """
        score = 0

        # Check for rights information
        if resource.get('rights'):
            score += 5

        # Check for file size
        if resource.get('size'):
            score += 5

        # Check for issue date
        if resource.get('created'):
            score += 5

        # Check for modification date
        if resource.get('metadata_modified'):
            score += 5

        return score

    def _calculate_resource_criteria_satisfaction(self, resource: Dict[str, Any]) -> Dict[str, bool]:
        """
        Determine which criteria are satisfied for a resource.

        Returns a dictionary with boolean values indicating whether each criterion is satisfied.

        Args:
            resource: The resource dictionary

        Returns:
            A dictionary of criteria satisfaction
        """
        format_in_vocab = bool(resource.get('format') and self._is_format_in_vocabulary(resource.get('format')))
        media_type_in_vocab = bool(resource.get('mimetype') and self._is_mimetype_in_vocabulary(resource.get('mimetype')))

        license_url = self._get_resource_license_url(resource)

        criteria = {
            # Accessibility criteria
            'has_access_url': bool(resource.get('url')),
            'access_url_accessible': self._check_url_accessibility(resource.get('url')),
            'has_download_url': bool(resource.get('download_url')),
            'download_url_accessible': bool(resource.get('download_url') and self._check_url_accessibility(resource.get('download_url'))),

            # Format quality criteria
            'has_format': bool(resource.get('format')),
            'has_media_type': bool(resource.get('mimetype')),
            'format_media_type_in_vocabulary': format_in_vocab or media_type_in_vocab,
            'is_open_format': resource.get('format', '').lower() in self.open_formats,
            'is_machine_readable': resource.get('format', '').lower() in self.machine_readable_formats,

            # Metadata completeness criteria
            'has_license': bool(license_url),
            'has_license_in_vocabulary': bool(license_url and self._license_in_registry(license_url)),

            # Documentation criteria
            'has_rights': bool(resource.get('rights')),
            'has_size': bool(resource.get('size')),
            'has_issue_date': bool(resource.get('issued') or resource.get('created')),
            'has_modification_date': bool(resource.get('metadata_modified') )
        }

        return criteria

    def _calculate_dataset_criteria_satisfaction(self, resources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate dataset-level criteria satisfaction based on all resources.

        For each criterion, calculates the percentage of resources that satisfy it.

        Args:
            resources: List of resource dictionaries

        Returns:
            A dictionary with criterion names as keys and satisfaction percentages as values
        """
        if not resources:
            return {}

        # Initialize counters for each criterion
        criteria_counts = {
            'has_access_url': 0,
            'access_url_accessible': 0,
            'has_download_url': 0,
            'download_url_accessible': 0,
            'has_format': 0,
            'has_media_type': 0,
            'format_media_type_in_vocabulary': 0,
            'is_open_format': 0,
            'is_machine_readable': 0,
            'has_license': 0,
            'has_license_in_vocabulary': 0,
            'has_rights': 0,
            'has_size': 0,
            'has_issue_date': 0,
            'has_modification_date': 0
        }

        # Count resources that satisfy each criterion
        for resource in resources:
            criteria = self._calculate_resource_criteria_satisfaction(resource)
            for criterion, satisfied in criteria.items():
                if satisfied:
                    criteria_counts[criterion] += 1

        # Calculate percentages
        num_resources = len(resources)
        criteria_percentages = {
            criterion: (count / num_resources) * 100 
            for criterion, count in criteria_counts.items()
        }

        # Group criteria by category
        format_quality_metrics = [
            criteria_percentages['has_format'],
            criteria_percentages['has_media_type'],
            criteria_percentages['format_media_type_in_vocabulary'],
            criteria_percentages['is_open_format'],
            criteria_percentages['is_machine_readable']
        ]

        result = {
            'accessibility': {
                'has_access_url': criteria_percentages['has_access_url'],
                'access_url_accessible': criteria_percentages['access_url_accessible'],
                'has_download_url': criteria_percentages['has_download_url'],
                'download_url_accessible': criteria_percentages['download_url_accessible'],
                'percentage': (
                    criteria_percentages['has_access_url'] + 
                    criteria_percentages['access_url_accessible'] + 
                    criteria_percentages['has_download_url'] + 
                    criteria_percentages['download_url_accessible']
                ) / 4
            },
            'format_quality': {
                'has_format': criteria_percentages['has_format'],
                'has_media_type': criteria_percentages['has_media_type'],
                'format_media_type_in_vocabulary': criteria_percentages['format_media_type_in_vocabulary'],
                'is_open_format': criteria_percentages['is_open_format'],
                'is_machine_readable': criteria_percentages['is_machine_readable'],
                'percentage': sum(format_quality_metrics) / len(format_quality_metrics)
            },
            'metadata_completeness': {
                'has_license': criteria_percentages['has_license'],
                'has_license_in_vocabulary': criteria_percentages['has_license_in_vocabulary'],
                'percentage': (
                    (criteria_percentages['has_license'] * 2) +
                    criteria_percentages['has_license_in_vocabulary']
                ) / 3
            },
            'documentation': {
                'has_rights': criteria_percentages['has_rights'],
                'has_size': criteria_percentages['has_size'],
                'has_issue_date': criteria_percentages['has_issue_date'],
                'has_modification_date': criteria_percentages['has_modification_date'],
                'percentage': (
                    criteria_percentages['has_rights'] + 
                    criteria_percentages['has_size'] + 
                    criteria_percentages['has_issue_date'] + 
                    criteria_percentages['has_modification_date']
                ) / 4
            }
        }

        # Calculate overall percentage
        result['overall_percentage'] = (
            result['accessibility']['percentage'] + 
            result['format_quality']['percentage'] + 
            result['metadata_completeness']['percentage'] + 
            result['documentation']['percentage']
        ) / 4

        return result

    def get_quality_level(self, percentage: float) -> Literal['excellent', 'good', 'sufficient', 'poor']:
        """
        Determine the quality level based on a percentage score.

        Args:
            percentage: The percentage score (0-100)

        Returns:
            The quality level: 'excellent', 'good', 'sufficient', or 'poor'
        """
        if percentage >= 80:
            return 'excellent'
        elif percentage >= 60:
            return 'good'
        elif percentage >= 40:
            return 'sufficient'
        else:
            return 'poor'

    def format_percentage(self, value: float) -> str:
        """
        Format a percentage value for display.

        Args:
            value: The percentage value to format

        Returns:
            The formatted percentage string (e.g., "75%")
        """
        return f"{value:.0f}%"

    @staticmethod
    def fmt_num(x: Union[float, int]) -> str:
        """
        Format a number for display, removing trailing zeros after the decimal point.

        If the number is an integer (i.e., has no decimal part or the decimal part is zero),
        it will be displayed as an integer. Otherwise, it will be displayed with one decimal place.

        Examples:
            50.0 -> "50"
            50.5 -> "50.5"
            50 -> "50"

        Args:
            x: The number to format (float or int)

        Returns:
            The formatted number as a string
        """
        if isinstance(x, int):
            return str(x)
        elif isinstance(x, float):
            if x.is_integer():
                return str(int(x))
            else:
                return f"{x:.1f}"
        else:
            # Handle any other type by converting to string
            return str(x)

    def check_dataset_property(self, dataset_dict: Dict[str, Any], property_path: str) -> bool:
        """
        Check if a dataset property exists and has a non-empty value.

        Args:
            dataset_dict: The dataset dictionary
            property_path: The property path to check (e.g., 'tags', 'organization.name')

        Returns:
            True if the property exists and has a non-empty value, False otherwise
        """
        parts = property_path.split('.')
        current = dataset_dict

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False

        # Check if the value is non-empty
        if isinstance(current, list):
            return len(current) > 0
        elif isinstance(current, dict):
            return bool(current)
        else:
            return bool(current)

    def prepare_display_data(self, dataset_dict: Dict[str, Any], mqa_scores: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare data for display in the template.

        This method processes the MQA scores and dataset dictionary to create a clean
        data structure that can be used directly in the template without complex logic.

        Args:
            dataset_dict: The dataset dictionary
            mqa_scores: The MQA scores dictionary

        Returns:
            A dictionary with display data for the template
        """
        display_data = {
            'overall': {
                'score': mqa_scores['total'],
                'formatted_score': self.fmt_num(mqa_scores['total']),
                'max_score': mqa_scores['max_score'],
                'formatted_max_score': self.fmt_num(mqa_scores['max_score']),
                'percentage': mqa_scores['percentage'],
                'formatted_percentage': self.format_percentage(mqa_scores['percentage']),
                'quality_level': self.get_quality_level(mqa_scores['percentage'])
            },
            'dimensions': {
                'findability': {
                    'score': mqa_scores['findability'],
                    'formatted_score': self.fmt_num(mqa_scores['findability']),
                    'max_score': 100,
                    'formatted_max_score': self.fmt_num(100),
                    'percentage': (mqa_scores['findability'] / 100) * 100,
                    'formatted_percentage': self.format_percentage((mqa_scores['findability'] / 100) * 100),
                    'criteria': {
                        'has_keywords': self.check_dataset_property(dataset_dict, 'tags'),
                        'has_categories': self.check_dataset_property(dataset_dict, 'groups') or bool(dataset_dict.get('theme')),
                        'has_spatial': bool(dataset_dict.get('spatial') or dataset_dict.get('spatial_uri') or dataset_dict.get('spatial_coverage')),
                        'has_temporal': bool(dataset_dict.get('temporal_start') or dataset_dict.get('temporal_end') or dataset_dict.get('temporal') or dataset_dict.get('temporal_coverage'))
                    }
                },
                'accessibility': {
                    'score': mqa_scores['accessibility'],
                    'formatted_score': self.fmt_num(mqa_scores['accessibility']),
                    'max_score': 100,
                    'formatted_max_score': self.fmt_num(100),
                    'percentage': (mqa_scores['accessibility'] / 100) * 100,
                    'formatted_percentage': self.format_percentage((mqa_scores['accessibility'] / 100) * 100)
                },
                'interoperability': {
                    'score': mqa_scores['interoperability'],
                    'formatted_score': self.fmt_num(mqa_scores['interoperability']),
                    'max_score': 110,
                    'formatted_max_score': self.fmt_num(110),
                    'percentage': (mqa_scores['interoperability'] / 110) * 100,
                    'formatted_percentage': self.format_percentage((mqa_scores['interoperability'] / 110) * 100),
                    'criteria': {
                        'is_dcat_ap_compliant': self._check_dcat_ap_compliance(dataset_dict)
                    }
                },
                'reusability': {
                    'score': mqa_scores['reusability'],
                    'formatted_score': self.fmt_num(mqa_scores['reusability']),
                    'max_score': 75,
                    'formatted_max_score': self.fmt_num(75),
                    'percentage': (mqa_scores['reusability'] / 75) * 100,
                    'formatted_percentage': self.format_percentage((mqa_scores['reusability'] / 75) * 100),
                    'criteria': {
                        'has_access_rights': bool(dataset_dict.get('access_rights')),
                        'access_rights_in_vocabulary': self.is_access_right_in_vocabulary(dataset_dict.get('access_rights', '')),
                        'has_contact_point': self.has_valid_contact_point(dataset_dict),
                        'has_publisher': bool(dataset_dict.get('organization') or dataset_dict.get('publisher'))
                    }
                },
                'contextuality': {
                    'score': mqa_scores['contextuality'],
                    'formatted_score': self.fmt_num(mqa_scores['contextuality']),
                    'max_score': 20,
                    'formatted_max_score': self.fmt_num(20),
                    'percentage': (mqa_scores['contextuality'] / 20) * 100,
                    'formatted_percentage': self.format_percentage((mqa_scores['contextuality'] / 20) * 100)
                }
            },
            'distributions': []
        }

        # Process distribution data
        if mqa_scores.get('distributions'):
            for dist in mqa_scores['distributions']:
                distribution_data = {
                    'id': dist.get('resource_id', ''),
                    'name': dist.get('name', ''),
                    'format': dist.get('format', ''),
                    'score': dist.get('total', 0),
                    'formatted_score': self.fmt_num(dist.get('total', 0)),
                    'max_score': dist.get('max_score', 0),
                    'formatted_max_score': self.fmt_num(dist.get('max_score', 0)),
                    'percentage': dist.get('percentage', 0),
                    'formatted_percentage': self.format_percentage(dist.get('percentage', 0)),
                    'quality_level': self.get_quality_level(dist.get('percentage', 0)),
                    'criteria': dist.get('criteria', {}),
                    'most_frequent_access_url_status_codes': dist.get('most_frequent_access_url_status_codes', []),
                    'most_frequent_download_url_status_codes': dist.get('most_frequent_download_url_status_codes', [])
                }
                display_data['distributions'].append(distribution_data)

            # Add dataset-level criteria from the first distribution
            if display_data['distributions'] and 'dataset_criteria' in mqa_scores['distributions'][0]:
                display_data['dataset_criteria'] = mqa_scores['distributions'][0]['dataset_criteria']

        return display_data
