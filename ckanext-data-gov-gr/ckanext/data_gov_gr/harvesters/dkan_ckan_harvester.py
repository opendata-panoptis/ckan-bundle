import logging
import re
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urlencode

from ckan.lib.helpers import json
from ckan.model import meta
from ckan.plugins import toolkit
from ckanext.data_gov_gr import helpers as data_gov_helpers

from ckanext.data_gov_gr.harvesters.core_ckan_harvester import CoreCkanHarvester
from ckanext.harvest.harvesters.ckanharvester import (
    SearchError,
    ContentFetchError,
)

try:
    from ckanext.vocabulary_admin.model.tag_metadata import VocabularyTagMetadata
except Exception:
    VocabularyTagMetadata = None

try:
    from unidecode import unidecode
except Exception:
    unidecode = None

log = logging.getLogger(__name__)


class DkanCkanHarvester(CoreCkanHarvester):
    """
    Harvester for DKAN-based portals that expose the CKAN dataset API via the
    ``current_package_list_with_resources`` endpoint.
    """

    DEFAULT_PAGE_SIZE = 100
    METADATA_RE = re.compile(r"metadata_modified:\[(?P<since>[^Z]+)Z TO \*\]")
    DATE_PREFIXES = (
        "date changed ",
        "last updated ",
        "last modified ",
        "updated on ",
    )
    KNOWN_DATE_FORMATS = (
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
    )
    SIZE_PATTERN = re.compile(r"^(?P<number>[0-9]+(?:[\.,][0-9]+)?)\s*(?P<unit>[a-zA-Z]*)$")
    VALID_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9\-_]*$")
    _media_type_lookup = None
    _license_lookup = None
    LICENSE_URL_ID_MAP = {
        "http://opendefinition.org/licenses/odc-odbl/": "odc-odbl",
        "https://opendefinition.org/licenses/odc-odbl/": "odc-odbl",
        "http://opendatacommons.org/licenses/odbl/": "odc-odbl",
        "https://opendatacommons.org/licenses/odbl/": "odc-odbl",
        "http://opendatacommons.org/licenses/by/": "odc-by",
        "https://opendatacommons.org/licenses/by/": "odc-by",
        "http://opendatacommons.org/licenses/pddl/": "odc-pddl",
        "https://opendatacommons.org/licenses/pddl/": "odc-pddl",
    }
    LICENSE_ID_NORMALIZATION = {
        "odc-odbl": "odc-odbl",
        "odc_odbl": "odc-odbl",
        "odc/by": "odc-by",
        "odc-by": "odc-by",
        "odc_by": "odc-by",
        "odc-pddl": "odc-pddl",
        "odc_pddl": "odc-pddl",
        "cc-by": "cc-by",
        "cc-by-sa": "cc-by-sa",
        "cc-by-nd": "cc-by-nd",
        "cc-by-nc": "cc-by-nc",
        "cc-by-nc-sa": "cc-by-nc-sa",
        "cc-by-nc-nd": "cc-by-nc-nd",
        "cc0": "cc-zero",
        "cc-zero": "cc-zero",
        "uk-ogl": "uk-ogl",
        "ogl": "uk-ogl",
        "other-open": "other-open",
        "other-pd": "other-pd",
        "other-at": "other-at",
        "gfdl": "gfdl",
        "cc0-1.0": "CC0",
        "cc-by-4.0": "CC_BY_4_0",
        "cc-by-sa-4.0": "CC_BYSA_4_0",
        "cc-by-nd-4.0": "CC_BYND_4_0",
        "cc-by-nc-4.0": "CC_BYNC_4_0",
        "odbl-1.0": "ODC_BL",
        "odc-by-1.0": "ODC_BY",
        "pddl-1.0": "ODC_PDDL",
        "eupl-1.2": "EUPL_1_2",
        "apache-2.0": "APACHE_2_0",
        "mit": "MIT",
        "gpl-3.0-only": "GPL_3_0",
        "lgpl-3.0-only": "LGPL_3_0",
        "agpl-3.0-only": "AGPL_3_0",
        "mpl-2.0": "MPL_2_0",
        "ogl-uk-3.0": "OGL_3_0",
        "ogl-uk-2.0": "OGL_2_0",
        "ogl-uk-1.0": "OGL_1_0",
        "etalab-2.0": "ETALAB_2_0",
        "dl-de-by-2.0": "DLDE_BY_2_0",
        "dl-de-zero-2.0": "DLDE_ZERO_2_0",
        "nlod-2.0": "NLOD_2_0",
        "cc-by-3.0": "CC_BY_3_0",
        "cc-by-sa-3.0": "CC_BYSA_3_0",
    }

    OPEN_LICENSE_IDS = {
        "odc-odbl",
        "odc-by",
        "odc-pddl",
        "cc-by",
        "cc-by-sa",
        "cc-by-nd",
        "cc-by-nc",
        "cc-by-nc-sa",
        "cc-by-nc-nd",
        "cc-zero",
        "uk-ogl",
        "other-open",
        "other-pd",
        "other-at",
        "gfdl",
    }

    def info(self):
        info = super(DkanCkanHarvester, self).info()
        info.update(
            {
                "name": "dkan_ckan_harvester",
                "title": "DKAN CKAN Harvester",
                "description": "Harvester for DKAN instances using "
                "current_package_list_with_resources",
            }
        )
        return info

    def _search_for_datasets(self, remote_ckan_base_url, fq_terms=None):
        """
        Retrieve datasets from DKAN instances, honouring CKAN harvester
        expectations while switching paging to ``limit``/``offset``.
        """
        base_search_url = (
            remote_ckan_base_url.rstrip("/")
            + self._get_action_api_offset()
            + "/current_package_list_with_resources"
        )

        page_size = self._get_page_size()
        include_orgs, exclude_orgs, include_groups, exclude_groups, metadata_since = (
            self._parse_filters(fq_terms or [])
        )

        pkg_dicts = []
        pkg_ids = set()
        offset = 0
        stop_fetching = False

        while True:
            params = {"limit": str(page_size), "offset": str(offset)}
            url = base_search_url + "?" + urlencode(params)
            log.debug("Fetching DKAN datasets: %s", url)

            try:
                content = self._get_content(url)
            except ContentFetchError as exc:
                raise SearchError(
                    "Error sending request to DKAN endpoint %s. Error: %s"
                    % (url, exc)
                )

            try:
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError("Response from DKAN was not JSON: %r" % content)

            pkg_dicts_page = self._extract_result(response_dict)

            if not pkg_dicts_page:
                break

            for pkg in pkg_dicts_page:
                if not isinstance(pkg, dict):
                    log.warning(
                        "Skipping dataset entry with unexpected type %s (offset=%s)",
                        type(pkg).__name__,
                        offset,
                    )
                    continue
                dataset_id = pkg.get("id")
                if not dataset_id:
                    log.warning("Skipping dataset without id (offset=%s)", offset)
                    continue
                if dataset_id in pkg_ids:
                    continue

                if not self._passes_org_filters(pkg, include_orgs, exclude_orgs):
                    continue

                if not self._passes_group_filters(pkg, include_groups, exclude_groups):
                    continue

                if metadata_since and self._is_older_than(pkg, metadata_since):
                    stop_fetching = True
                    continue

                pkg_ids.add(dataset_id)
                pkg_dicts.append(pkg)

            if stop_fetching or len(pkg_dicts_page) < page_size:
                break

            offset += page_size

        return pkg_dicts

    def modify_package_dict(self, package_dict, harvest_object):
        package_dict = super(DkanCkanHarvester, self).modify_package_dict(
            package_dict, harvest_object
        )

        # Όλα τα DKAN-harvested σύνολα δεδομένων θεωρούνται PUBLIC
        # στο data.gov.gr, άρα βάζουμε ρητά access_rights = PUBLIC.
        try:
            self._set_default_access_rights_public(package_dict)
        except Exception as e:
            log.error("Error forcing access_rights PUBLIC in DKAN harvester: %s", e)

        remote_package = {}
        try:
            if harvest_object and harvest_object.content:
                remote_package = json.loads(harvest_object.content)
        except Exception:
            log.warning(
                "Failed to parse harvest object content for dataset %s during DKAN sanitisation",
                package_dict.get("id", "unknown"),
            )
            remote_package = {}

        # Set core DKAN/DCAT fields based on remote data
        self._sanitize_package_flags(package_dict)
        self._sanitize_package_dates(package_dict, remote_package)
        self._sanitize_resource_metadata(package_dict, remote_package)
        self._ensure_package_license(package_dict, remote_package)
        self._ensure_package_name(package_dict, harvest_object, remote_package)
        self._ensure_contact_from_maintainer(package_dict)
        self._ensure_applicable_legislation(package_dict)
        self._ensure_landing_page(package_dict, remote_package)

        return package_dict

    def _get_page_size(self):
        config = self.config or {}
        candidates = [
            config.get("dkan_page_size"),
            config.get("page_size"),
            config.get("limit"),
            config.get("rows"),
        ]
        for candidate in candidates:
            try:
                value = int(candidate)
                if value > 0:
                    return value
            except (TypeError, ValueError):
                continue
        return self.DEFAULT_PAGE_SIZE

    def _parse_filters(self, fq_terms):
        include_orgs = None
        exclude_orgs = set()
        include_groups = None
        exclude_groups = set()
        metadata_since = None

        for raw_term in fq_terms:
            raw_term = raw_term.strip()
            if not raw_term:
                continue

            metadata_match = self.METADATA_RE.match(raw_term)
            if metadata_match and not metadata_since:
                metadata_since = self._parse_datetime(metadata_match.group("since"))

            parts = [term.strip() for term in raw_term.split(" OR ")]
            for term in parts:
                if term.startswith("-organization:"):
                    exclude_orgs.add(term.split(":", 1)[1])
                elif term.startswith("organization:"):
                    include_orgs = include_orgs or set()
                    include_orgs.add(term.split(":", 1)[1])
                elif term.startswith("-groups:"):
                    exclude_groups.add(term.split(":", 1)[1])
                elif term.startswith("groups:"):
                    include_groups = include_groups or set()
                    include_groups.add(term.split(":", 1)[1])

        return include_orgs, exclude_orgs, include_groups, exclude_groups, metadata_since

    def _extract_result(self, response_dict):
        result = response_dict.get("result")
        if isinstance(result, list):
            flattened = []
            for item in result:
                if isinstance(item, dict):
                    flattened.append(item)
                elif isinstance(item, list):
                    flattened.extend(
                        sub for sub in item if isinstance(sub, dict)
                    )
            return flattened
        if isinstance(result, dict):
            for key in ("results", "datasets"):
                value = result.get(key)
                if isinstance(value, list):
                    return value
        raise SearchError(
            "Response JSON did not contain dataset list: %r" % response_dict
        )

    def _passes_org_filters(self, pkg, include_orgs, exclude_orgs):
        if not include_orgs and not exclude_orgs:
            return True

        org = pkg.get("organization") or {}
        org_id = pkg.get("owner_org") or org.get("id")
        org_name = org.get("name")

        def matches(candidate):
            if not candidate:
                return False
            return (candidate == org_id) or (candidate == org_name)

        if include_orgs:
            if not any(matches(candidate) for candidate in include_orgs):
                return False

        if exclude_orgs:
            if any(matches(candidate) for candidate in exclude_orgs):
                return False

        return True

    def _sanitize_package_dates(self, package_dict, remote_package):
        if not isinstance(package_dict, dict):
            return

        for key in ("metadata_created", "metadata_modified", "revision_timestamp"):
            raw_value = package_dict.get(key)
            if not raw_value and isinstance(remote_package, dict):
                raw_value = remote_package.get(key)

            normalised = self._normalise_dkan_date(raw_value)
            if normalised:
                package_dict[key] = normalised
            elif key in package_dict:
                package_dict.pop(key, None)

    def _sanitize_resource_metadata(self, package_dict, remote_package):
        resources = package_dict.get("resources")
        if not isinstance(resources, list):
            return

        remote_resources = []
        if isinstance(remote_package, dict):
            remote_resources = remote_package.get("resources") or []

        for index, resource in enumerate(resources):
            if not isinstance(resource, dict):
                continue

            remote_resource = {}
            if index < len(remote_resources) and isinstance(remote_resources[index], dict):
                remote_resource = remote_resources[index]

            for field in ("created", "last_modified"):
                raw_value = resource.get(field) or remote_resource.get(field)
                normalised = self._normalise_dkan_date(raw_value)

                if normalised:
                    resource[field] = normalised
                else:
                    resource.pop(field, None)

            size_value = resource.get("size")
            if size_value is None and remote_resource:
                size_value = remote_resource.get("size")

            normalised_size = self._normalise_resource_size(size_value)
            if normalised_size is not None:
                resource["size"] = normalised_size
            else:
                resource.pop("size", None)

            revision_ts = resource.get("revision_timestamp") or remote_resource.get(
                "revision_timestamp"
            )
            normalised_revision = self._normalise_dkan_date(revision_ts)
            if normalised_revision:
                resource["revision_timestamp"] = normalised_revision
            else:
                resource.pop("revision_timestamp", None)

            normalised_state = self._normalise_state(resource.get("state"))
            if normalised_state:
                resource["state"] = normalised_state
            else:
                resource.pop("state", None)

            mimetype_value = resource.get("mimetype") or remote_resource.get("mimetype")
            format_hint = resource.get("format") or remote_resource.get("format")
            normalised_mimetype = self._normalise_mimetype(mimetype_value, format_hint)
            if normalised_mimetype:
                mapped_value = self._map_to_media_type(
                    normalised_mimetype, self._get_media_type_lookup()
                )
                if mapped_value:
                    resource["mimetype"] = mapped_value
                    log.debug(
                        "DKAN mimetype accepted for resource %s: %s",
                        resource.get("id") or resource.get("name"),
                        mapped_value,
                    )
                else:
                    log.debug(
                        "Removing mimetype '%s' for resource %s because it is not in Media types vocabulary",
                        normalised_mimetype,
                        resource.get("id") or resource.get("name"),
                    )
                    resource.pop("mimetype", None)
            else:
                resource.pop("mimetype", None)

    def _normalise_dkan_date(self, value):
        if not value:
            return None

        if isinstance(value, datetime):
            candidate = value
        elif isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None

            lowered = candidate.lower()
            for prefix in self.DATE_PREFIXES:
                if lowered.startswith(prefix):
                    candidate = candidate[len(prefix):].strip()
                    lowered = candidate.lower()
                    break

            # Remove leading weekday (e.g. "Wed, ") if present
            if "," in candidate:
                first, remainder = candidate.split(",", 1)
                if first.strip().isalpha():
                    candidate = remainder.strip()

            candidate = candidate.rstrip("Z").strip()
            candidate = re.sub(r"\s*\(.*?\)\s*$", "", candidate)
            iso_candidate = candidate

            try:
                parsed = datetime.fromisoformat(iso_candidate)
            except ValueError:
                # Try again after normalising separators
                candidate = candidate.replace("T", " ")
                candidate = candidate.replace(" - ", " ")
                candidate = re.sub(r"\s+", " ", candidate)
                parsed = None

                for fmt in self.KNOWN_DATE_FORMATS:
                    try:
                        parsed = datetime.strptime(candidate, fmt)
                        break
                    except ValueError:
                        continue

                if parsed is None:
                    return None
            else:
                parsed = parsed

            if parsed.tzinfo:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)

            return parsed.strftime("%Y-%m-%d %H:%M:%S")

        else:
            return None

        if candidate.tzinfo:
            candidate = candidate.astimezone(timezone.utc).replace(tzinfo=None)

        return candidate.strftime("%Y-%m-%d %H:%M:%S")

    def _normalise_resource_size(self, value):
        if value in (None, ""):
            return None

        if isinstance(value, (int, float)):
            try:
                return max(int(value), 0)
            except (ValueError, OverflowError):
                return None

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            if stripped.isdigit():
                return int(stripped)

            match = self.SIZE_PATTERN.match(stripped.lower())
            if not match:
                return None

            number_raw = match.group("number").replace(",", ".")
            try:
                number = float(number_raw)
            except ValueError:
                return None

            unit = match.group("unit").lower()
            multipliers = {
                "": 1,
                "b": 1,
                "byte": 1,
                "bytes": 1,
                "k": 1024,
                "kb": 1024,
                "kib": 1024,
                "m": 1024 ** 2,
                "mb": 1024 ** 2,
                "mib": 1024 ** 2,
                "g": 1024 ** 3,
                "gb": 1024 ** 3,
                "gib": 1024 ** 3,
                "t": 1024 ** 4,
                "tb": 1024 ** 4,
                "tib": 1024 ** 4,
            }

            multiplier = multipliers.get(unit)
            if multiplier is None:
                return None

            size = int(number * multiplier)
            if size < 0:
                return None
            return size

        return None

    def _sanitize_package_flags(self, package_dict):
        if not isinstance(package_dict, dict):
            return

        normalised_private = self._normalise_private(package_dict.get("private"))
        if normalised_private is None:
            package_dict.pop("private", None)
        else:
            package_dict["private"] = normalised_private

        normalised_state = self._normalise_state(package_dict.get("state"))
        if normalised_state:
            package_dict["state"] = normalised_state
        else:
            package_dict.pop("state", None)

        normalised_type = self._normalise_dataset_type(package_dict.get("type"))
        if normalised_type:
            package_dict["type"] = normalised_type

    def _normalise_private(self, value):
        if value is None:
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        if isinstance(value, str):
            lowered = value.strip().lower()
            mapping = {
                "true": True,
                "1": True,
                "yes": True,
                "private": True,
                "draft": True,
                "unpublished": True,
                "false": False,
                "0": False,
                "no": False,
                "public": False,
                "published": False,
                "open": False,
            }
            if lowered in mapping:
                return mapping[lowered]

        return False

    def _normalise_state(self, value):
        if not value:
            return "active"

        if isinstance(value, str):
            lowered = value.strip().lower()
            mapping = {
                "active": "active",
                "published": "active",
                "open": "active",
                "draft": "draft",
                "inactive": "draft",
                "pending": "draft",
                "deleted": "deleted",
                "archived": "deleted",
            }
            return mapping.get(lowered, "active")

        return "active"

    def _normalise_dataset_type(self, value):
        if not value:
            return "dataset"

        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"dataset", "package"}:
                return "dataset"
            return lowered

        return "dataset"

    def _normalise_mimetype(self, value, format_hint=None):
        if not value and not format_hint:
            return None

        def _clean(candidate):
            if not candidate:
                return None
            candidate = str(candidate).strip()
            if not candidate:
                return None
            lowered = candidate.lower()
            if lowered.startswith("https://www.iana.org/assignments/media-types/"):
                lowered = lowered.split("/media-types/", 1)[-1]
            return lowered

        value = _clean(value)
        format_hint = _clean(format_hint)

        mapping = {
            "csv": "text/csv",
            "xls": "application/vnd.ms-excel",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xlsm": "application/vnd.ms-excel.sheet.macroenabled.12",
            "xlb": "application/vnd.ms-excel",
            "xlsb": "application/vnd.ms-excel.sheet.binary.macroenabled.12",
            "json": "application/json",
            "geojson": "application/geo+json",
            "xml": "application/xml",
            "html": "text/html",
            "txt": "text/plain",
            "kml": "application/vnd.google-earth.kml+xml",
            "kmz": "application/vnd.google-earth.kmz",
            "shp": "application/octet-stream",
            "zip": "application/zip",
            "pdf": "application/pdf",
            "bin": "application/octet-stream",
            "ods": "application/vnd.oasis.opendocument.spreadsheet",
        }

        lookup = self._get_media_type_lookup()

        candidates = []

        def _add_candidate(candidate):
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        if value and "/" in value:
            _add_candidate(value)
        elif value:
            mapped = mapping.get(value)
            if mapped:
                _add_candidate(mapped)

        if format_hint:
            if "/" in format_hint:
                _add_candidate(format_hint)
            else:
                mapped = mapping.get(format_hint)
                if mapped:
                    _add_candidate(mapped)

        if not candidates and (value or format_hint):
            _add_candidate("application/octet-stream")

        for candidate in candidates:
            mapped_value = self._map_to_media_type(candidate, lookup)
            if mapped_value:
                return mapped_value

        return None

    def _ensure_package_name(self, package_dict, harvest_object, remote_package):
        current_name = package_dict.get("name")
        if self._is_valid_name(current_name):
            return

        candidates = []
        if isinstance(remote_package, dict):
            candidates.extend([
                remote_package.get("name"),
                remote_package.get("id"),
            ])

        if harvest_object:
            candidates.extend([harvest_object.guid, getattr(harvest_object, "id", None)])

        candidates.append(package_dict.get("id"))

        for candidate in candidates:
            slug = self._slugify_name(candidate)
            if self._is_valid_name(slug):
                package_dict["name"] = slug
                return

        fallback = f"dataset-{harvest_object.id}" if harvest_object else "dataset"
        slug = self._slugify_name(fallback)
        package_dict["name"] = slug or fallback

    def _is_valid_name(self, value):
        if not value or not isinstance(value, str):
            return False
        return bool(self.VALID_NAME_RE.match(value))

    def _slugify_name(self, value):
        if not value:
            return None

        if not isinstance(value, str):
            value = str(value)

        if unidecode:
            ascii_value = unidecode(value)
        else:
            normalised = unicodedata.normalize("NFKD", value)
            ascii_value = normalised.encode("ascii", "ignore").decode("ascii")

        ascii_value = ascii_value.lower()
        ascii_value = re.sub(r"[^a-z0-9]+", "-", ascii_value)
        ascii_value = ascii_value.strip("-")
        if not ascii_value:
            return None
        # Collapse repeated dashes
        ascii_value = re.sub(r"-+", "-", ascii_value)
        return ascii_value

    def _ensure_applicable_legislation(self, package_dict):
        """
        Ensure that the dataset has an ``applicable_legislation`` field set.

        For DKAN-harvested datasets on data.gov.gr τα σύνολα δεδομένων
        θεωρούνται πάντα PUBLIC, οπότε χρησιμοποιούμε μόνο την προεπιλογή
        για ανοιχτά δεδομένα:

        - PUBLIC datasets -> ``ckanext.data_gov_gr.dataset.legislation.open``
        """
        if not isinstance(package_dict, dict):
            return

        # Do not override an explicit value if it already exists
        existing = package_dict.get("applicable_legislation")
        if existing:
            return

        access_rights = package_dict.get("access_rights")
        if not isinstance(access_rights, str):
            return

        access_lower = access_rights.strip().lower()
        # Εφαρμόζουμε προεπιλεγμένη νομοθεσία μόνο όταν είναι PUBLIC
        if not (access_lower.endswith("/public") or "access-right/public" in access_lower):
            return

        value = data_gov_helpers.get_config_value(
            "ckanext.data_gov_gr.dataset.legislation.open", ""
        )
        if not isinstance(value, str):
            return

        value = value.strip()
        if not value:
            return

        # Schema expects a multiple_text field, so store as a list
        package_dict["applicable_legislation"] = [value]

    def _ensure_contact_from_maintainer(self, package_dict):
        maintainer = package_dict.get("maintainer")
        maintainer_email = package_dict.get("maintainer_email")

        if not maintainer and not maintainer_email:
            return

        try:
            contacts = package_dict.get("contact")
            if isinstance(contacts, dict):
                contacts = [contacts]
            elif not isinstance(contacts, list):
                contacts = []

            def _matches(contact):
                if not isinstance(contact, dict):
                    return False
                return (
                    (maintainer and contact.get("name") == maintainer)
                    or (maintainer_email and contact.get("email") == maintainer_email)
                )

            existing_contact = next((c for c in contacts if _matches(c)), None)
            if existing_contact:
                if maintainer and not existing_contact.get("name"):
                    existing_contact["name"] = maintainer
                if maintainer_email and not existing_contact.get("email"):
                    existing_contact["email"] = maintainer_email
            else:
                new_contact = {}
                if maintainer:
                    new_contact["name"] = maintainer
                if maintainer_email:
                    new_contact["email"] = maintainer_email
                if new_contact:
                    contacts.append(new_contact)

            contacts = [c for c in contacts if isinstance(c, dict) and c]
            if contacts:
                package_dict["contact"] = contacts
        except Exception as e:
            log.error("Error syncing maintainer into contact point: %s", e)

    def _ensure_landing_page(self, package_dict, remote_package):
        """
        Χρησιμοποιεί το πεδίο ``url`` από το απομακρυσμένο DKAN dataset
        ως ``landing_page`` στο data.gov.gr, αν δεν έχει ήδη οριστεί.
        """
        if not isinstance(package_dict, dict):
            return

        # Μην πειράζεις αν υπάρχει ήδη landing_page (π.χ. από άλλο harvester logic)
        if package_dict.get("landing_page"):
            return

        if not isinstance(remote_package, dict):
            return

        remote_url = remote_package.get("url")
        if isinstance(remote_url, str):
            remote_url = remote_url.strip()

        if remote_url:
            package_dict["landing_page"] = remote_url

    def _map_to_media_type(self, mime_value, lookup):
        """
        Map an incoming mime value to a canonical Media Types vocabulary entry.

        DKAN sources often send short values like ``text/csv``. Our CKAN schema
        expects the full IANA URL (eg.
        ``https://www.iana.org/assignments/media-types/text/csv``). We only
        return a value if the final string exists verbatim in the vocabulary;
        otherwise the mimetype is dropped so the dataset passes validation.
        """
        if not mime_value or not lookup:
            return None

        raw_value = str(mime_value).strip()
        if not raw_value:
            return None

        base_url = "https://www.iana.org/assignments/media-types/"

        def _resolve(candidate):
            if not candidate:
                return None
            key = candidate.strip().rstrip("/").lower()
            return lookup.get(key)

        # If the remote value already looks like a canonical IANA URL, use it
        # directly (after normalising trailing slashes/case).
        resolved = None
        if raw_value.lower().startswith(base_url):
            resolved = _resolve(raw_value)
            if resolved and resolved.lower().startswith(base_url):
                return resolved
            # Fall through to rebuild from the extracted code

        code = self._extract_media_type_code(raw_value)
        if not code:
            return None

        canonical = self._build_iana_url(code)
        resolved = _resolve(canonical)
        if resolved and resolved.lower().startswith(base_url):
            return resolved

        # Some vocabularies might have stored the canonical value with a trailing
        # slash; try that variant as well.
        resolved = _resolve(canonical.rstrip("/") + "/")
        if resolved and resolved.lower().startswith(base_url):
            return resolved

        return None

    def _extract_media_type_code(self, value):
        if not value:
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

    def _get_media_type_lookup(self):
        if DkanCkanHarvester._media_type_lookup is not None:
            return DkanCkanHarvester._media_type_lookup

        lookup = {}
        try:
            vocabulary = toolkit.get_action("vocabulary_show")(
                {"ignore_auth": True}, {"id": "Media types"}
            )

            tags = vocabulary.get("tags", []) if isinstance(vocabulary, dict) else []
            metadata_map = {}

            if VocabularyTagMetadata and tags:
                try:
                    tag_ids = [tag.get("id") for tag in tags if tag.get("id")]
                    if tag_ids:
                        metadata_entries = (
                            meta.Session.query(VocabularyTagMetadata)
                            .filter(VocabularyTagMetadata.tag_id.in_(tag_ids))
                            .all()
                        )
                        metadata_map = {entry.tag_id: entry for entry in metadata_entries}
                except Exception as exc:
                    log.error("Error loading Media types tag metadata: %s", exc)

            actual_names = {}
            for tag in tags:
                name = tag.get("name") if isinstance(tag, dict) else None
                if not name:
                    continue
                trimmed = name.strip()
                if not trimmed:
                    continue
                tag_id = tag.get("id")
                metadata_entry = metadata_map.get(tag_id) if tag_id else None

                preferred_value = None
                if metadata_entry and getattr(metadata_entry, "value_uri", None):
                    preferred_value = metadata_entry.value_uri.strip()

                if not preferred_value:
                    preferred_value = trimmed

                actual_names[preferred_value.rstrip("/").lower()] = preferred_value

            base_url = "https://www.iana.org/assignments/media-types/"

            for canonical_lower, display_value in actual_names.items():
                code = self._extract_media_type_code(display_value)
                preferred_value = None

                # If the stored tag is already an IANA URL we trust it.
                if display_value.lower().startswith(base_url):
                    preferred_value = display_value
                else:
                    # Otherwise, only accept it when the vocabulary also contains
                    # the canonical IANA URL form; we avoid fabricating values.
                    candidate_url = self._build_iana_url(code)
                    candidate_key = candidate_url.rstrip("/").lower()
                    preferred_value = actual_names.get(candidate_key)
                    if not preferred_value:
                        continue

                preferred_key = preferred_value.rstrip("/").lower()
                lookup[preferred_key] = preferred_value
                if code:
                    lookup[self._build_iana_url(code).rstrip("/").lower()] = preferred_value
        except toolkit.ObjectNotFound:
            log.warning("Media types vocabulary not found; mimetype values will be dropped if invalid")
        except Exception as exc:
            log.error(f"Error loading Media types vocabulary: {exc}")

        DkanCkanHarvester._media_type_lookup = lookup
        return lookup

    def _get_license_lookup(self):
        if DkanCkanHarvester._license_lookup is not None:
            return DkanCkanHarvester._license_lookup

        lookup = {"by_id": {}, "by_url": {}}
        try:
            licenses = toolkit.get_action("license_list")(
                {"ignore_auth": True}, {}
            )
            for license_obj in licenses:
                eu_url = None
                license_id = (license_obj.get("id") or "").strip()
                if license_id:
                    entry = dict(license_obj)
                    eu_url = self._map_license_to_eu_uri(
                        license_obj.get("url"),
                        license_id,
                    )
                    if eu_url:
                        entry["eu_url"] = eu_url
                        lookup["by_url"][eu_url.rstrip("/").lower()] = eu_url
                    lookup["by_id"][license_id.lower()] = entry
        except Exception as exc:
            log.warning("Could not load license registry: %s", exc)

        DkanCkanHarvester._license_lookup = lookup
        return lookup

    def _is_media_type_allowed(self, value):
        if not value:
            return False

        lookup = self._get_media_type_lookup()
        if not lookup:
            return False

        candidate = value.strip()
        if not candidate:
            return False

        lowered = candidate.lower()
        return lowered in lookup

    def _build_iana_url(self, code):
        if not code:
            return ''
        cleaned = str(code).strip().strip('/')
        if not cleaned:
            return ''
        if cleaned.lower().startswith('https://www.iana.org/assignments/media-types/'):
            return cleaned
        return f"https://www.iana.org/assignments/media-types/{cleaned}"

    def _canonicalize_license(self, license_url, license_id, license_lookup):
        license_by_id = license_lookup.get("by_id", {}) if isinstance(license_lookup, dict) else {}
        license_by_url = license_lookup.get("by_url", {}) if isinstance(license_lookup, dict) else {}

        candidates = []
        if license_url:
            candidates.append(license_url)
        if license_url or license_id:
            mapped = self._map_license_to_eu_uri(license_url, license_id)
            if mapped:
                candidates.append(mapped)

        if license_id:
            entry = license_by_id.get(license_id.strip().lower())
            if entry:
                if entry.get("eu_url"):
                    candidates.append(entry["eu_url"])

        for candidate in candidates:
            if not candidate:
                continue
            normalized = candidate.rstrip("/").lower()
            if normalized in license_by_url:
                return license_by_url[normalized]

        return None

    def _ensure_package_license(self, package_dict, remote_package):
        if not isinstance(package_dict, dict) or not isinstance(remote_package, dict):
            return

        existing_license_id = package_dict.get("license_id")
        existing_license_title = package_dict.get("license_title")
        existing_license_url = package_dict.get("license_url")

        remote_license_id = remote_package.get("license_id")
        remote_license_title = remote_package.get("license_title")
        remote_license_url = remote_package.get("license_url")

        if not remote_license_url and self._looks_like_url(remote_license_title):
            remote_license_url = remote_license_title.strip()

        resolved_id = self._normalise_license_identifier(
            existing_license_id or remote_license_id,
            remote_license_title,
            remote_license_url,
        )

        if resolved_id and not existing_license_id:
            package_dict["license_id"] = resolved_id

        license_lookup = self._get_license_lookup()
        license_by_id = license_lookup.get("by_id", {})
        license_urls = license_lookup.get("by_url", {})

        canonical_license = None
        if resolved_id:
            canonical_license = license_by_id.get(resolved_id.lower())

        canonical_title = canonical_license.get("title") or canonical_license.get("id") if canonical_license else None
        canonical_url = None
        if canonical_license:
            canonical_url = (
                canonical_license.get("eu_url")
                or canonical_license.get("url")
            )

        if canonical_title:
            package_dict["license_title"] = canonical_title
        elif remote_license_title and not existing_license_title:
            package_dict["license_title"] = remote_license_title

        # Preserve the remote URL in license_url where possible. Only fall back to
        # canonical data when nothing was supplied.
        if existing_license_url:
            pass  # keep whatever was already there
        elif remote_license_url:
            package_dict["license_url"] = remote_license_url
        elif canonical_license and canonical_license.get("url"):
            package_dict["license_url"] = canonical_license["url"]

        effective_license_url = package_dict.get("license_url") or remote_license_url

        dataset_canonical_uri = self._canonicalize_license(
            effective_license_url,
            package_dict.get("license_id"),
            license_lookup,
        )

        package_dict.pop("license", None)

        if resolved_id and resolved_id in self.OPEN_LICENSE_IDS:
            package_dict["isopen"] = True

        try:
            self._add_license_to_resources(package_dict)
        except Exception as exc:
            log.error("Error updating resource licenses after DKAN normalisation: %s", exc)

        # Ensure resource-level licence fields inherit the final dataset values.
        resource_license_fields = {
            "license_id": package_dict.get("license_id"),
            "license_title": package_dict.get("license_title"),
            "license_url": package_dict.get("license_url"),
        }

        for resource in package_dict.get("resources") or []:
            if not isinstance(resource, dict):
                continue

            for field, value in resource_license_fields.items():
                if value and not resource.get(field):
                    resource[field] = value

            resource_canonical_uri = self._canonicalize_license(
                resource.get("license_url") or resource.get("license"),
                resource.get("license_id"),
                license_lookup,
            )

            if not resource_canonical_uri and dataset_canonical_uri:
                resource_canonical_uri = dataset_canonical_uri

            if resource_canonical_uri:
                resource["license"] = resource_canonical_uri
            else:
                resource.pop("license", None)

            # Mark resource explicitly open when dataset is open.
            if package_dict.get("isopen"):
                resource["is_open"] = True

    def _normalise_license_identifier(self, license_id, license_title, license_url):
        candidates = []
        for value in (license_id, license_title, license_url):
            if not isinstance(value, str):
                continue
            candidate = value.strip()
            if candidate:
                candidates.append(candidate)

        for candidate in candidates:
            lowered = candidate.lower().rstrip("/")
            mapped = self.LICENSE_ID_NORMALIZATION.get(lowered)
            if mapped:
                return mapped

        for candidate in candidates:
            lowered = candidate.lower().rstrip("/")
            for url, mapped in self.LICENSE_URL_ID_MAP.items():
                if lowered == url.rstrip("/"):
                    return mapped
            if "odc-odbl" in lowered or "odbl" in lowered:
                return "odc-odbl"
            if "odc-by" in lowered:
                return "odc-by"
            if "odc-pddl" in lowered or "pddl" in lowered:
                return "odc-pddl"
            if "cc-by-nc-nd" in lowered:
                return "cc-by-nc-nd"
            if "cc-by-nc-sa" in lowered:
                return "cc-by-nc-sa"
            if "cc-by-nc" in lowered:
                return "cc-by-nc"
            if "cc-by-sa" in lowered:
                return "cc-by-sa"
            if "cc-by-nd" in lowered:
                return "cc-by-nd"
            if "cc-by" in lowered:
                return "cc-by"
            if "cc0" in lowered or "creative commons zero" in lowered:
                return "cc-zero"
            if "ogl" in lowered:
                return "uk-ogl"
            if "open government licence" in lowered or "open government license" in lowered:
                return "uk-ogl"
            if "public domain" in lowered:
                return "other-pd"
            if "attribution" in lowered and "non" not in lowered:
                return "other-at"

        return None

    def _looks_like_url(self, value):
        if not isinstance(value, str):
            return False
        trimmed = value.strip().lower()
        return trimmed.startswith("http://") or trimmed.startswith("https://")

    def _passes_group_filters(self, pkg, include_groups, exclude_groups):
        if not include_groups and not exclude_groups:
            return True

        groups = pkg.get("groups") or []
        group_names = {group.get("name") for group in groups if group.get("name")}

        if include_groups:
            if not group_names.intersection(include_groups):
                return False

        if exclude_groups:
            if group_names.intersection(exclude_groups):
                return False

        return True

    def _is_older_than(self, pkg, metadata_since):
        metadata_modified = (
            pkg.get("metadata_modified") or pkg.get("metadata_created")
        )
        dataset_dt = self._parse_datetime(metadata_modified)
        if not dataset_dt:
            return False
        return dataset_dt < metadata_since

    def _parse_datetime(self, value):
        if not value:
            return None
        try:
            cleaned = value.strip()
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            dt_value = datetime.fromisoformat(cleaned)
            if not dt_value.tzinfo:
                dt_value = dt_value.replace(tzinfo=timezone.utc)
            return dt_value.astimezone(timezone.utc)
        except Exception as exc:
            log.debug("Could not parse datetime '%s': %s", value, exc)
            return None
