# ckanext/data_gov_gr/logic/harvest_mapping.py
from __future__ import annotations

from urllib.parse import urlparse, parse_qs
import json
import os
import re

from typing import Any, Iterable

import ckanext.data_gov_gr.helpers as helpers

NS = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "srv": "http://www.isotc211.org/2005/srv",
    "gml": "http://www.opengis.net/gml",
    "gml32": "http://www.opengis.net/gml/3.2",
    "gco": "http://www.isotc211.org/2005/gco",
    "gmx": "http://www.isotc211.org/2005/gmx",
    "xlink": "http://www.w3.org/1999/xlink",
}

LAYER_RESOURCE_BASE_URL_CONFIG_KEY = "layer_resource_base_url"
WMS_PREVIEW_FROM_ONLINE_RESOURCE_CONFIG_KEY = "wms_preview_from_online_resource"
WMS_PREVIEW_FROM_WMS_ONLINE_RESOURCES_CONFIG_KEY = (
    "wms_preview_from_wms_online_resources"
)
WMS_PREVIEW_BASE_URL_CONFIG_KEY = "wms_preview_base_url"
WMS_CAPABILITIES_URL_CONFIG_KEY = "wms_capabilities_url"
WFS_CAPABILITIES_URL_CONFIG_KEY = "wfs_capabilities_url"
PRESERVE_RESOURCE_IDS_BY_URL_CONFIG_KEY = "preserve_resource_ids_by_url"
SKIP_DATASET_WHEN_NO_NON_UUID_LAYER_IDENTIFIER_CONFIG_KEY = (
    "skip_dataset_when_no_non_uuid_layer_identifier"
)
SKIP_DATASET_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY = (
    "skip_dataset_when_title_matches_layer_name"
)
SKIP_DATA_SERVICE_RECORDS_CONFIG_KEY = "skip_data_service_records"
DATASET_NAME_PREFIX_FROM_FILE_IDENTIFIER_CONFIG_KEY = (
    "dataset_name_prefix_from_file_identifier"
)
LANDING_PAGE_BASE_URL_FROM_FILE_IDENTIFIER_CONFIG_KEY = (
    "landing_page_base_url_from_file_identifier"
)
SAFE_RESOURCE_FORMAT_INFERENCE_CONFIG_KEY = "safe_resource_format_inference"
DEFAULT_DATASET_FIELDS_CONFIG_KEY = "default_dataset_fields"
OVERRIDE_DEFAULT_DATASET_FIELDS_CONFIG_KEY = "override_default_dataset_fields"
DEFAULT_RESOURCE_FIELDS_CONFIG_KEY = "default_resource_fields"
OVERRIDE_DEFAULT_RESOURCE_FIELDS_CONFIG_KEY = "override_default_resource_fields"
RESOURCE_MIMETYPE_FROM_DISTRIBUTION_FORMAT_CONFIG_KEY = (
    "resource_mimetype_from_distribution_format"
)
RESOURCE_DESCRIPTION_FROM_NAME_CONFIG_KEY = "resource_description_from_name"
RESOURCE_ACCESS_URL_FROM_URL_CONFIG_KEY = "resource_access_url_from_url"
RESOURCE_RIGHTS_FROM_USE_CONSTRAINTS_CONFIG_KEY = (
    "resource_rights_from_use_constraints"
)
RESOURCE_RIGHTS_PLAIN_TEXT_FROM_USE_CONSTRAINTS_CONFIG_KEY = (
    "resource_rights_plain_text_from_use_constraints"
)
DATA_SERVICE_PACKAGE_TYPE = "data-service"
IANA_MEDIA_TYPE_BASE_URL = "https://www.iana.org/assignments/media-types/"
XML_MIMETYPE = IANA_MEDIA_TYPE_BASE_URL + "application/xml"
MEDIA_TYPES_VOCABULARY_NAME = "Media types"
INSPIRE_CONDITIONS_APPLYING_TO_ACCESS_AND_USE = (
    "http://inspire.ec.europa.eu/metadata-codelist/"
    "ConditionsApplyingToAccessAndUse/"
)
_UUID_LIKE_RE = re.compile(
    r"^[a-z]?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_LAYER_IDENTIFIER_UUID_LIKE_RE = re.compile(
    r"^[a-z]?[0-9a-f]{7,8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _is_data_service_package(package_dict: dict[str, Any]) -> bool:
    return str(package_dict.get("type") or "").strip() == DATA_SERVICE_PACKAGE_TYPE


def should_skip_data_service_package(
    package_dict: dict[str, Any],
    harvest_object: Any = None,
) -> bool:
    """
    Returns True when configured to skip CSW records imported as data-service
    packages by ckanext-spatial.

    Enabled only with harvest source config:
      {"skip_data_service_records": true}
    """
    if not isinstance(package_dict, dict):
        return False

    config = get_harvest_source_config(harvest_object)
    if not config.get(SKIP_DATA_SERVICE_RECORDS_CONFIG_KEY):
        return False

    return _is_data_service_package(package_dict)

EU_THEME_URI = {
    "AGRI": "http://publications.europa.eu/resource/authority/data-theme/AGRI",
    "EDUC": "http://publications.europa.eu/resource/authority/data-theme/EDUC",
    "ENER": "http://publications.europa.eu/resource/authority/data-theme/ENER",
    "ENVI": "http://publications.europa.eu/resource/authority/data-theme/ENVI",
    "GOVE": "http://publications.europa.eu/resource/authority/data-theme/GOVE",
    "HEAL": "http://publications.europa.eu/resource/authority/data-theme/HEAL",
    "INTR": "http://publications.europa.eu/resource/authority/data-theme/INTR",
    "JUST": "http://publications.europa.eu/resource/authority/data-theme/JUST",
    "REGI": "http://publications.europa.eu/resource/authority/data-theme/REGI",
    "SOCI": "http://publications.europa.eu/resource/authority/data-theme/SOCI",
    "TECH": "http://publications.europa.eu/resource/authority/data-theme/TECH",
    "TRAN": "http://publications.europa.eu/resource/authority/data-theme/TRAN",
    "ECON": "http://publications.europa.eu/resource/authority/data-theme/ECON",
}

TOPICCATEGORY_TO_EU_THEME = {
    "biota": ["ENVI"],
    "climatologyMeteorologyAtmosphere": ["ENVI"],
    "environment": ["ENVI"],
    "geoscientificInformation": ["ENVI"],
    "elevation": ["ENVI"],
    "inlandWaters": ["ENVI"],
    "oceans": ["ENVI"],
    "imageryBaseMapsEarthCover": ["REGI"],
    "farming": ["AGRI"],
    "society": ["SOCI"],
    "health": ["HEAL"],
    "economy": ["ECON"],
    "transportation": ["TRAN"],
    "planningCadastre": ["REGI"],
    "boundaries": ["REGI"],
    "location": ["REGI"],
    "structure": ["REGI"],
    "intelligenceMilitary": ["GOVE"],
    "utilitiesCommunication": ["ENER", "TECH"],
}

# ISO 19115-1 MD_MaintenanceFrequencyCode -> EU frequency URI
ISO_MAINTFREQ_TO_EU_FREQ_URI = {
    "annually": "http://publications.europa.eu/resource/authority/frequency/ANNUAL",
    "biannually": "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2",
    "biennially": "http://publications.europa.eu/resource/authority/frequency/BIENNIAL",
    "continual": "http://publications.europa.eu/resource/authority/frequency/CONT",
    "daily": "http://publications.europa.eu/resource/authority/frequency/DAILY",
    "fortnightly": "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY",
    "irregular": "http://publications.europa.eu/resource/authority/frequency/IRREG",
    "monthly": "http://publications.europa.eu/resource/authority/frequency/MONTHLY",
    "notPlanned": "http://publications.europa.eu/resource/authority/frequency/NOT_PLANNED",
    "periodic": "http://publications.europa.eu/resource/authority/frequency/OTHER",
    "quarterly": "http://publications.europa.eu/resource/authority/frequency/QUARTERLY",
    "semimonthly": "http://publications.europa.eu/resource/authority/frequency/MONTHLY_2",
    "unknown": "http://publications.europa.eu/resource/authority/frequency/UNKNOWN",
    "weekly": "http://publications.europa.eu/resource/authority/frequency/WEEKLY",
    "asNeeded": "http://publications.europa.eu/resource/authority/frequency/AS_NEEDED",
}


DEFAULT_OPEN_LEGISLATION = "https://eur-lex.europa.eu/eli/dir/2019/1024/oj/eng"
DEFAULT_PROTECTED_LEGISLATION = "https://eur-lex.europa.eu/eli/dir/2019/1024/oj/eng"


# -----------------------------------------------------------------------------
# DCAT dataset-type mapping
# -----------------------------------------------------------------------------

GEOSPATIAL_DCAT_TYPE = "http://publications.europa.eu/resource/authority/dataset-type/GEOSPATIAL"


def apply_dcat_type_geospatial(
    package_dict: dict[str, Any],
    *,
    overwrite: bool = False,
) -> None:
    """
    Ensures package_dict['dcat_type'] is set to the GEOSPATIAL dataset-type URI.

    By default does NOT overwrite existing dcat_type.
    """
    if not isinstance(package_dict, dict):
        return

    if not overwrite and package_dict.get("dcat_type"):
        return

    package_dict["dcat_type"] = GEOSPATIAL_DCAT_TYPE


# -----------------------------------------------------------------------------
# Dataset name mapping from fileIdentifier
# -----------------------------------------------------------------------------

def normalize_ckan_name(value: str) -> str:
    """
    Normalizes a value for CKAN package name usage.
    """
    s = (value or "").strip().lower()
    if not s:
        return ""

    s = re.sub(r"[^a-z0-9_-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-_")


def extract_file_identifier(xml_tree) -> str:
    """
    Extracts gmd:fileIdentifier/gco:CharacterString from ISO19139 XML.
    """
    if xml_tree is None:
        return ""

    values = xml_tree.xpath(
        "//gmd:fileIdentifier/gco:CharacterString/text()",
        namespaces=NS,
    )
    for value in values:
        file_identifier = str(value).strip()
        if file_identifier:
            return file_identifier

    return ""


def apply_dataset_name_from_file_identifier(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> None:
    """
    Sets package_dict['name'] from configured prefix + ISO fileIdentifier.

    Config:
      {"dataset_name_prefix_from_file_identifier": "gis-peiraias-"}

    The rule is disabled unless the config key is present and non-empty.
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    default_extras = config.get("default_extras")
    prefix = config.get(DATASET_NAME_PREFIX_FROM_FILE_IDENTIFIER_CONFIG_KEY)
    if not prefix and isinstance(default_extras, dict):
        prefix = default_extras.get(DATASET_NAME_PREFIX_FROM_FILE_IDENTIFIER_CONFIG_KEY)
    prefix = str(prefix or "")
    if not prefix.strip():
        return

    file_identifier = extract_file_identifier(xml_tree)
    if not file_identifier:
        return

    dataset_name = normalize_ckan_name(f"{prefix}{file_identifier}")
    if dataset_name:
        package_dict["name"] = dataset_name


def apply_landing_page_from_file_identifier(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> None:
    """
    Sets package_dict['landing_page'] from configured base URL + ISO fileIdentifier.

    Config:
      {"landing_page_base_url_from_file_identifier": "https://.../metadata/"}

    The rule is disabled unless the config key is present and non-empty.
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    base_url = str(
        config.get(LANDING_PAGE_BASE_URL_FROM_FILE_IDENTIFIER_CONFIG_KEY) or ""
    )
    if not base_url.strip():
        return

    file_identifier = extract_file_identifier(xml_tree)
    if not file_identifier:
        return

    package_dict["landing_page"] = [f"{base_url}{file_identifier}"]


def apply_default_dataset_fields_from_config(
    package_dict: dict[str, Any],
    harvest_object: Any = None,
) -> None:
    """
    Applies default package_dict fields from harvest source config.

    Config:
      {
        "default_dataset_fields": {
          "hvd_category": ["http://data.europa.eu/bna/c_ac64a52d"]
        },
        "override_default_dataset_fields": false
      }

    By default existing non-empty values are preserved.
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    defaults = config.get(DEFAULT_DATASET_FIELDS_CONFIG_KEY)
    if not isinstance(defaults, dict) or not defaults:
        return

    overwrite = bool(config.get(OVERRIDE_DEFAULT_DATASET_FIELDS_CONFIG_KEY))
    for field_name, value in defaults.items():
        if not isinstance(field_name, str) or not field_name.strip():
            continue

        if not overwrite and package_dict.get(field_name):
            continue

        package_dict[field_name] = value


def apply_default_resource_fields_from_config(
    package_dict: dict[str, Any],
    harvest_object: Any = None,
) -> None:
    """
    Applies default resource fields from harvest source config.

    Config:
      {
        "default_resource_fields": {
          "license": "http://publications.europa.eu/resource/authority/licence/CC_BY_4_0"
        },
        "override_default_resource_fields": false
      }

    By default existing non-empty values are preserved.
    """
    if not isinstance(package_dict, dict):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list) or not resources:
        return

    config = get_harvest_source_config(harvest_object)
    defaults = config.get(DEFAULT_RESOURCE_FIELDS_CONFIG_KEY)
    if not isinstance(defaults, dict) or not defaults:
        return

    overwrite = bool(config.get(OVERRIDE_DEFAULT_RESOURCE_FIELDS_CONFIG_KEY))
    for resource in resources:
        if not isinstance(resource, dict):
            continue

        for field_name, value in defaults.items():
            if not isinstance(field_name, str) or not field_name.strip():
                continue

            if not overwrite and resource.get(field_name):
                continue

            resource[field_name] = value


# -----------------------------------------------------------------------------
# Resource mimetype mapping (ISO distributionFormat -> resource mimetype)
# -----------------------------------------------------------------------------

def extract_distribution_format_iana_mimetype(xml_tree) -> str:
    """
    Extracts the first IANA media type URI from ISO19139 distribution format.
    """
    if xml_tree is None:
        return ""

    values = xml_tree.xpath(
        (
            "//gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/"
            "gmd:MD_Format/gmd:name/gco:CharacterString/text()"
        ),
        namespaces=NS,
    )

    for value in values:
        mimetype = str(value).strip()
        if mimetype.startswith(IANA_MEDIA_TYPE_BASE_URL):
            return mimetype

    return ""


def _is_iana_mimetype_in_media_types_vocabulary(mimetype_url: str) -> bool:
    """
    Returns True only when Media types vocabulary has matching tag value_uri.
    """
    if not mimetype_url:
        return False

    try:
        import ckan.plugins.toolkit as toolkit

        vocabulary_data = toolkit.get_action("vocabularyadmin_vocabulary_show")(
            {"ignore_auth": True},
            {"id": MEDIA_TYPES_VOCABULARY_NAME},
        )
    except Exception:
        return False

    tags = vocabulary_data.get("tags", []) if isinstance(vocabulary_data, dict) else []
    return any(
        isinstance(tag, dict) and tag.get("value_uri") == mimetype_url
        for tag in tags
    )


def apply_resource_mimetype_from_distribution_format(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
    *,
    overwrite: bool = False,
) -> None:
    """
    Sets every resource['mimetype'] from ISO distribution format IANA URI.

    The rule is disabled unless the harvest source config contains:
      {"resource_mimetype_from_distribution_format": true}

    The extracted URI is applied only if it exists as value_uri in the
    "Media types" vocabulary.
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    if not config.get(RESOURCE_MIMETYPE_FROM_DISTRIBUTION_FORMAT_CONFIG_KEY):
        return

    mimetype = extract_distribution_format_iana_mimetype(xml_tree)
    if not _is_iana_mimetype_in_media_types_vocabulary(mimetype):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        return

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        if not overwrite and resource.get("mimetype"):
            continue
        resource["mimetype"] = mimetype


def apply_resource_description_from_name(
    package_dict: dict[str, Any],
    harvest_object: Any = None,
) -> None:
    """
    Sets missing resource['description_translated'] from resource name.

    The rule is disabled unless the harvest source config contains:
      {"resource_description_from_name": true}
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    if not config.get(RESOURCE_DESCRIPTION_FROM_NAME_CONFIG_KEY):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        return

    def translated_value_has_text(value: Any) -> bool:
        if isinstance(value, dict):
            return any(str(v or "").strip() for v in value.values())
        return bool(str(value or "").strip())

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        if translated_value_has_text(resource.get("description_translated")):
            continue

        name = str(resource.get("name") or "").strip()
        name_translated = resource.get("name_translated")
        if isinstance(name_translated, dict):
            name_el = str(name_translated.get("el") or "").strip()
            name_en = str(name_translated.get("en") or "").strip()
        else:
            name_el = name
            name_en = name

        if not name_el and name:
            name_el = name
        if not name_en and name:
            name_en = name
        if not name_el and name_en:
            name_el = name_en
        if not name_en and name_el:
            name_en = name_el
        if not name_el and not name_en:
            continue

        resource["description_translated"] = {
            "el": name_el,
            "en": name_en,
        }


def apply_resource_access_url_from_url(
    package_dict: dict[str, Any],
    harvest_object: Any = None,
    *,
    overwrite: bool = False,
) -> None:
    """
    Sets missing resource['access_url'] from resource['url'].

    The rule is disabled unless the harvest source config contains:
      {"resource_access_url_from_url": true}
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    if not config.get(RESOURCE_ACCESS_URL_FROM_URL_CONFIG_KEY):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        return

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        if not overwrite and str(resource.get("access_url") or "").strip():
            continue

        url = str(resource.get("url") or "").strip()
        if not url:
            continue

        resource["access_url"] = url


# -----------------------------------------------------------------------------
# Tags cleanup (remove junk tags like "__", "--", "____", "----")
# -----------------------------------------------------------------------------

def cleanup_package_tags(package_dict: dict[str, Any]) -> None:
    """
    Removes invalid/junk tags from package_dict['tags'].

    Specifically removes tags that are:
      - empty/whitespace
      - composed ONLY of digits and/or underscores and/or hyphens
        e.g. "__", "--", "123", "2024-01-01", "12__34--5"
    """
    if not isinstance(package_dict, dict):
        return

    tags = package_dict.get("tags")
    if not isinstance(tags, list) or not tags:
        return

    import re
    only_digits_underscores_hyphens = re.compile(r"^[0-9_-]+$")

    cleaned: list[Any] = []
    for t in tags:
        if isinstance(t, dict):
            name = t.get("name")
            if name is None:
                continue
            s = str(name).strip()
            if not s or only_digits_underscores_hyphens.match(s):
                continue
            t["name"] = s
            cleaned.append(t)
        else:
            s = str(t).strip()
            if not s or only_digits_underscores_hyphens.match(s):
                continue
            cleaned.append(s)

    package_dict["tags"] = cleaned


# -----------------------------------------------------------------------------
# Theme mapping (ISO topicCategory -> DCAT theme)
# -----------------------------------------------------------------------------

def extract_topic_categories(iso_values: dict[str, Any], xml_tree) -> list[str]:
    # 1) try iso_values
    v = iso_values.get("topic-category") or iso_values.get("topicCategory")
    if isinstance(v, list):
        out = [str(x).strip() for x in v if x is not None and str(x).strip()]
        if out:
            return out
    elif isinstance(v, str) and v.strip():
        return [v.strip()]

    # 2) fallback to XPath
    if xml_tree is None:
        return []

    nodes = xml_tree.xpath(
        (
            "//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/"
            "gmd:MD_TopicCategoryCode/text()"
            " | "
            "//gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:topicCategory/"
            "gmd:MD_TopicCategoryCode/text()"
        ),
        namespaces=NS,
    )
    return [str(n).strip() for n in nodes if n is not None and str(n).strip()]


def map_topiccategories_to_theme_uris(topic_categories: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    for tc in topic_categories:
        key = (tc or "").strip()
        if not key:
            continue

        for code in TOPICCATEGORY_TO_EU_THEME.get(key, []):
            uri = EU_THEME_URI.get(code)
            if uri and uri not in seen:
                seen.add(uri)
                out.append(uri)

    return out


def apply_theme_from_topiccategory(
    package_dict: dict[str, Any],
    iso_values: dict[str, Any],
    xml_tree,
) -> None:
    topic_categories = extract_topic_categories(iso_values, xml_tree)
    theme_uris = map_topiccategories_to_theme_uris(topic_categories)
    if theme_uris:
        package_dict["theme"] = theme_uris


# -----------------------------------------------------------------------------
# License - Rights mapping
# -----------------------------------------------------------------------------

_EU_LICENCE_BASE = "http://publications.europa.eu/resource/authority/licence/"
_EU_LICENCE_VOCAB_CACHE: dict[str, Any] | None = None


def _normalize_url(u: str) -> str:
    if not u:
        return ""
    s = str(u).strip().replace("&amp;", "&")
    while s and s[-1] in ".)]>,":  # remove common trailing punctuation from free text
        s = s[:-1]
    if s.endswith("/"):
        s = s[:-1]
    return s


def _extract_first_url(text: str) -> str:
    if not text:
        return ""
    import re
    m = re.search(r"https?://[^\s)>\]]+", text)
    return _normalize_url(m.group(0)) if m else ""


def _load_eu_licence_vocab() -> dict[str, Any]:
    """
    Loads EU licence SKOS RDF (RDF/XML) and builds lookup tables.

    File location (close to harvesting logic):
      ckanext/data_gov_gr/logic/vocab/licences-skos.rdf
    """
    global _EU_LICENCE_VOCAB_CACHE
    if _EU_LICENCE_VOCAB_CACHE is not None:
        return _EU_LICENCE_VOCAB_CACHE

    by_exactmatch_url: dict[str, str] = {}
    by_identifier: dict[str, str] = {}
    title_by_uri: dict[str, str] = {}

    try:
        import os
        from lxml import etree

        this_dir = os.path.dirname(__file__)
        rdf_path = os.path.abspath(os.path.join(this_dir, "vocab", "licences-skos.rdf"))

        if not os.path.exists(rdf_path):
            _EU_LICENCE_VOCAB_CACHE = {
                "by_exactmatch_url": by_exactmatch_url,
                "by_identifier": by_identifier,
                "title_by_uri": title_by_uri,
            }
            return _EU_LICENCE_VOCAB_CACHE

        root = etree.parse(rdf_path).getroot()

        ns = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "dc": "http://purl.org/dc/elements/1.1/",
        }

        for c in root.xpath("//skos:Concept", namespaces=ns):
            eu_uri = (c.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about") or "").strip()
            if not eu_uri.startswith(_EU_LICENCE_BASE):
                continue

            ids = c.xpath("dc:identifier/text()", namespaces=ns)
            for i in ids:
                key = str(i).strip().lower()
                if key:
                    by_identifier[key] = eu_uri

            pref_en = c.xpath("skos:prefLabel[@xml:lang='en']/text()", namespaces=ns)
            if pref_en:
                title_by_uri[eu_uri] = str(pref_en[0]).strip()

            exact = c.xpath("skos:exactMatch/@rdf:resource", namespaces=ns)
            for x in exact:
                nu = _normalize_url(str(x)).lower()
                if nu:
                    by_exactmatch_url[nu] = eu_uri

    except Exception:
        pass

    _EU_LICENCE_VOCAB_CACHE = {
        "by_exactmatch_url": by_exactmatch_url,
        "by_identifier": by_identifier,
        "title_by_uri": title_by_uri,
    }
    return _EU_LICENCE_VOCAB_CACHE


def map_license_url_to_eu_licence_uri(license_url: str) -> str:
    """
    Returns EU Publications Office licence URI if mapped, else ''.
    """
    u = _normalize_url(license_url)
    if not u:
        return ""

    if u.startswith(_EU_LICENCE_BASE):
        return u

    vocab = _load_eu_licence_vocab()

    # direct exactMatch
    eu = vocab.get("by_exactmatch_url", {}).get(u.lower(), "")
    if eu:
        return eu

    # heuristic: creativecommons url -> identifier (e.g. CC_BY_3_0, CC_BYNC_4_0)
    import re
    cc = re.search(r"creativecommons\.org/licenses/([a-z\-]+)/(\d\.\d)", u, re.IGNORECASE)
    if cc:
        code = cc.group(1).upper().replace("-", "_")
        ver = cc.group(2).replace(".", "_")
        candidate = f"CC_{code}_{ver}".lower()
        return vocab.get("by_identifier", {}).get(candidate, "")

    if re.search(r"creativecommons\.org/publicdomain/zero/1\.0", u, re.IGNORECASE):
        return vocab.get("by_identifier", {}).get("cc0", "")

    return ""


def extract_iso_legal_constraints_license(xml_tree) -> dict[str, str]:
    """
    Extracts:
      - rights_text from otherConstraints (where useConstraints indicates license)
      - license_url: first URL found inside rights_text
    """
    if xml_tree is None:
        return {"rights_text": "", "license_url": ""}

    nodes = xml_tree.xpath(
        (
            "//gmd:resourceConstraints/gmd:MD_LegalConstraints["
            "gmd:useConstraints/gmd:MD_RestrictionCode/@codeListValue='license'"
            " or normalize-space(gmd:useConstraints/gmd:MD_RestrictionCode/text())='license'"
            "]"
        ),
        namespaces=NS,
    )

    texts: list[str] = []
    for n in nodes:
        parts = n.xpath(
            (
                ".//gmd:otherConstraints/gco:CharacterString/text()"
                " | .//gmd:otherConstraints/gmx:Anchor/text()"
                " | .//gmd:otherConstraints/*/text()"
            ),
            namespaces=NS,
        )
        for p in parts:
            s = str(p).strip()
            if s:
                texts.append(s)

    # de-duplicate preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for t in texts:
        if t and t not in seen:
            seen.add(t)
            deduped.append(t)

    rights_text = "\n\n".join(deduped).strip()
    if len(rights_text) > 8000:
        rights_text = rights_text[:8000] + " ...[TRUNCATED]"

    return {"rights_text": rights_text, "license_url": _extract_first_url(rights_text)}


def extract_iso_use_constraints_other_restrictions_rights(xml_tree) -> str:
    """
    Extracts INSPIRE access/use condition rights from ISO19139 LegalConstraints.

    Only reads useConstraints=otherRestrictions. accessConstraints is intentionally
    ignored. For gmx:Anchor values, the returned rights value combines the URI and
    label, separated by a blank line.
    """
    if xml_tree is None:
        return ""

    nodes = xml_tree.xpath(
        (
            "//gmd:resourceConstraints/gmd:MD_LegalConstraints["
            "gmd:useConstraints/gmd:MD_RestrictionCode/"
            "@codeListValue='otherRestrictions'"
            " or normalize-space(gmd:useConstraints/"
            "gmd:MD_RestrictionCode/text())='otherRestrictions'"
            "]"
        ),
        namespaces=NS,
    )

    rights_values: list[str] = []
    for n in nodes:
        constraints = n.xpath(".//gmd:otherConstraints", namespaces=NS)
        for constraint in constraints:
            uri = ""
            hrefs = constraint.xpath(".//@xlink:href", namespaces=NS)
            for href in hrefs:
                value = _normalize_url(str(href))
                if value.startswith(INSPIRE_CONDITIONS_APPLYING_TO_ACCESS_AND_USE):
                    uri = value
                    break

            if not uri:
                continue

            label = ""
            labels = constraint.xpath(".//text()", namespaces=NS)
            for text in labels:
                value = str(text).strip()
                if value:
                    label = value
                    break

            rights_values.append(f"{uri}\n\n{label}" if label else uri)

    seen: set[str] = set()
    deduped: list[str] = []
    for value in rights_values:
        if value and value not in seen:
            seen.add(value)
            deduped.append(value)

    rights_text = "\n\n".join(deduped).strip()
    if len(rights_text) > 8000:
        rights_text = rights_text[:8000] + " ...[TRUNCATED]"

    return rights_text


def extract_iso_use_constraints_other_restrictions_plain_text_rights(xml_tree) -> dict[str, str]:
    """
    Extracts plain-text rights from useConstraints=otherRestrictions.

    This is a fallback for sources that put licence/use-condition text directly
    in gco:CharacterString / LocalisedCharacterString instead of using the
    INSPIRE ConditionsApplyingToAccessAndUse codelist URI. accessConstraints is
    intentionally ignored.
    """
    if xml_tree is None:
        return {"rights_text": "", "license_url": ""}

    nodes = xml_tree.xpath(
        (
            "//gmd:resourceConstraints/gmd:MD_LegalConstraints["
            "gmd:useConstraints/gmd:MD_RestrictionCode/"
            "@codeListValue='otherRestrictions'"
            " or normalize-space(gmd:useConstraints/"
            "gmd:MD_RestrictionCode/text())='otherRestrictions'"
            "]"
        ),
        namespaces=NS,
    )

    rights_values: list[str] = []
    for n in nodes:
        constraints = n.xpath(".//gmd:otherConstraints", namespaces=NS)
        for constraint in constraints:
            parts = constraint.xpath(
                (
                    "./gco:CharacterString/text()"
                    " | .//gmd:LocalisedCharacterString/text()"
                ),
                namespaces=NS,
            )
            for part in parts:
                value = str(part).strip()
                if value:
                    rights_values.append(value)

    seen: set[str] = set()
    deduped: list[str] = []
    for value in rights_values:
        if value and value not in seen:
            seen.add(value)
            deduped.append(value)

    rights_text = "\n\n".join(deduped).strip()
    if len(rights_text) > 8000:
        rights_text = rights_text[:8000] + " ...[TRUNCATED]"

    return {"rights_text": rights_text, "license_url": _extract_first_url(rights_text)}


def apply_resource_rights_and_license_from_iso19139(
    package_dict: dict[str, Any],
    xml_tree,
    *,
    overwrite: bool = False,
) -> None:
    """
    Writes top-level resource fields:
      - resource['rights']
      - resource['license'] (ONLY if mapped to EU vocab URI)
      - resource['license_url']   (extracted URL)
      - resource['license_title'] (ALWAYS equals license_url)

    Note: if overwrite=True, clears resource['license'] when mapping fails.
    """
    if not isinstance(package_dict, dict):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list) or not resources:
        return

    data = extract_iso_legal_constraints_license(xml_tree)
    rights_text = (data.get("rights_text") or "").strip()
    extracted_url = (data.get("license_url") or "").strip()

    if not rights_text and not extracted_url:
        return

    eu_uri = map_license_url_to_eu_licence_uri(extracted_url) if extracted_url else ""

    for res in resources:
        if not isinstance(res, dict):
            continue

        # rights (top-level)
        if rights_text and (overwrite or not res.get("rights")):
            res["rights"] = rights_text

        # license_url + license_title (top-level)
        if extracted_url:
            if overwrite or not res.get("license_url"):
                res["license_url"] = extracted_url

            # license_title ALWAYS equals the URL
            if overwrite or not res.get("license_title"):
                res["license_title"] = extracted_url

        # license (top-level, only if mapped)
        if eu_uri:
            if overwrite or not res.get("license"):
                res["license"] = eu_uri
        else:
            if overwrite:
                res.pop("license", None)


def apply_resource_rights_from_iso_use_constraints(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
    *,
    overwrite: bool = False,
) -> None:
    """
    Writes resource['rights'] from useConstraints=otherRestrictions.

    INSPIRE ConditionsApplyingToAccessAndUse values are preferred. If none are
    found, plain-text otherConstraints can be used as a configurable fallback.
    accessConstraints are intentionally ignored.
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    if config.get(RESOURCE_RIGHTS_FROM_USE_CONSTRAINTS_CONFIG_KEY) is False:
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list) or not resources:
        return

    rights_text = extract_iso_use_constraints_other_restrictions_rights(xml_tree)
    extracted_url = ""
    if (
        not rights_text
        and config.get(
            RESOURCE_RIGHTS_PLAIN_TEXT_FROM_USE_CONSTRAINTS_CONFIG_KEY
        ) is not False
    ):
        data = extract_iso_use_constraints_other_restrictions_plain_text_rights(xml_tree)
        rights_text = (data.get("rights_text") or "").strip()
        extracted_url = (data.get("license_url") or "").strip()

    if not rights_text:
        return

    eu_uri = map_license_url_to_eu_licence_uri(extracted_url) if extracted_url else ""

    for res in resources:
        if not isinstance(res, dict):
            continue

        if overwrite or not res.get("rights"):
            res["rights"] = rights_text

        if extracted_url:
            if overwrite or not res.get("license_url"):
                res["license_url"] = extracted_url
            if overwrite or not res.get("license_title"):
                res["license_title"] = extracted_url
        if eu_uri and (overwrite or not res.get("license")):
            res["license"] = eu_uri


# -----------------------------------------------------------------------------
# Temporal coverage mapping (ISO EX_TemporalExtent -> temporal_coverage)
# -----------------------------------------------------------------------------

def _normalize_iso_date(value: str) -> str:
    """
    Normalizes ISO-ish date/datetime strings to YYYY-MM-DD.
    Examples:
      '2020-01-01' -> '2020-01-01'
      '2020-01-01T00:00:00+00:00' -> '2020-01-01'
      '2020-01-01T00:00:00Z' -> '2020-01-01'
    Returns '' if not usable.
    """
    if not value:
        return ""
    s = str(value).strip()
    if not s:
        return ""

    # If datetime, keep only date part
    if "T" in s:
        s = s.split("T", 1)[0].strip()

    # Basic sanity: keep only YYYY-MM-DD
    if len(s) >= 10:
        s10 = s[:10]
        # very lightweight check
        if s10[4:5] == "-" and s10[7:8] == "-":
            return s10

    return ""


def extract_temporal_coverage(xml_tree) -> list[dict[str, str]]:
    """
    Extract temporal coverage from ISO19139 XML tree.

    Supports:
      - gmd:EX_TemporalExtent//gml:TimePeriod(beginPosition/endPosition)
      - gmd:EX_TemporalExtent//gml:TimeInstant(timePosition) (mapped as start=end)

    Returns:
      [{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}] (may return multiple extents)
    """
    if xml_tree is None:
        return []

    # We use local-name + namespace-uri to be resilient to prefixes and gml vs gml32.
    # After your healing rule, most should be gml/3.2, but we accept both.
    te_nodes = xml_tree.xpath(
        "//*[namespace-uri()=$gmd and local-name()='EX_TemporalExtent']",
        gmd=NS["gmd"],
    )

    out: list[dict[str, str]] = []

    for te in te_nodes:
        # 1) TimePeriod: begin/end
        begins = te.xpath(
            ".//*[local-name()='TimePeriod']"
            "//*[local-name()='beginPosition']/text()",
        )
        ends = te.xpath(
            ".//*[local-name()='TimePeriod']"
            "//*[local-name()='endPosition']/text()",
        )

        # Pair by index if both exist; also allow open-ended extents.
        max_len = max(len(begins), len(ends))
        for i in range(max_len):
            b = _normalize_iso_date(begins[i]) if i < len(begins) else ""
            e = _normalize_iso_date(ends[i]) if i < len(ends) else ""
            if b or e:
                out.append({"start": b, "end": e})

        # 2) TimeInstant: timePosition -> start=end
        instants = te.xpath(
            ".//*[local-name()='TimeInstant']"
            "//*[local-name()='timePosition']/text()",
        )
        for t in instants:
            d = _normalize_iso_date(t)
            if d:
                out.append({"start": d, "end": d})

    return out


def apply_temporal_coverage_from_iso19139(
    package_dict: dict[str, Any],
    xml_tree,
    *,
    overwrite: bool = False,
) -> None:
    """
    Sets package_dict['temporal_coverage'] from ISO19139 XML.
    By default does not overwrite an existing temporal_coverage.
    """
    if not isinstance(package_dict, dict):
        return

    if not overwrite and package_dict.get("temporal_coverage"):
        return

    temporal_coverage = extract_temporal_coverage(xml_tree)
    if temporal_coverage:
        package_dict["temporal_coverage"] = temporal_coverage


# -----------------------------------------------------------------------------
# Frequency mapping (ISO resourceMaintenance -> DCAT frequency)
# -----------------------------------------------------------------------------

def extract_maintenance_frequency(iso_values: dict[str, Any], xml_tree) -> str:
    """
    Returns ISO MD_MaintenanceFrequencyCode value (eg 'asNeeded') or ''.
    Prefers iso_values (produced by ckanext-spatial ISO document mapping).
    """
    v = (
        iso_values.get("frequency-of-update")
        or iso_values.get("frequency_of_update")
        or iso_values.get("maintenance_frequency")
    )

    if isinstance(v, str) and v.strip():
        return v.strip()

    if xml_tree is None:
        return ""

    nodes = xml_tree.xpath(
        (
            "//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/"
            "gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/"
            "gmd:MD_MaintenanceFrequencyCode/@codeListValue"
            " | "
            "//gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/"
            "gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/"
            "gmd:MD_MaintenanceFrequencyCode/@codeListValue"
            " | "
            "//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/"
            "gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/"
            "gmd:MD_MaintenanceFrequencyCode/text()"
            " | "
            "//gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/"
            "gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/"
            "gmd:MD_MaintenanceFrequencyCode/text()"
        ),
        namespaces=NS,
    )
    for n in nodes:
        s = str(n).strip()
        if s:
            return s
    return ""


def map_maintenance_frequency_to_eu_uri(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    return ISO_MAINTFREQ_TO_EU_FREQ_URI.get(v, "")


def apply_frequency_from_resource_maintenance(
    package_dict: dict[str, Any],
    iso_values: dict[str, Any],
    xml_tree,
    *,
    overwrite: bool = False,
) -> None:
    """
    Sets package_dict['frequency'] to the EU Publications Office frequency URI.

    By default it does NOT overwrite an existing package_dict['frequency'].
    """
    if not isinstance(package_dict, dict):
        return

    if not overwrite and package_dict.get("frequency"):
        return

    iso_freq = extract_maintenance_frequency(iso_values, xml_tree)
    eu_uri = map_maintenance_frequency_to_eu_uri(iso_freq)
    if eu_uri:
        package_dict["frequency"] = eu_uri


# -----------------------------------------------------------------------------
# Legislation mapping (defaults from config + fallback)
# -----------------------------------------------------------------------------

def get_legislation_value(*, protected: bool = False) -> str:
    fallback = DEFAULT_PROTECTED_LEGISLATION if protected else DEFAULT_OPEN_LEGISLATION
    key = (
        "ckanext.data_gov_gr.dataset.legislation.protected"
        if protected
        else "ckanext.data_gov_gr.dataset.legislation.open"
    )

    try:
        v = helpers.get_config_value(key, fallback)
        if isinstance(v, str):
            v = v.strip()
        if not v:
            return fallback
        return str(v)
    except Exception:
        return fallback


def ensure_applicable_legislation(
    package_dict: dict[str, Any],
    *,
    protected: bool = False,
) -> None:
    """
    Adds package_dict['applicable_legislation'] if missing/empty.
    Does NOT overwrite existing values.
    """
    if not isinstance(package_dict, dict):
        return

    existing = package_dict.get("applicable_legislation")
    if existing:
        return

    value = get_legislation_value(protected=protected)
    if value:
        package_dict["applicable_legislation"] = [value]


# -----------------------------------------------------------------------------
# Publisher mapping (source/landing_page rules)
# -----------------------------------------------------------------------------

def apply_cityofathens_publisher(package_dict: dict[str, Any], harvest_object: Any = None) -> None:
    """
    If the harvest source URL (or landing_page) indicates City of Athens,
    set a fixed publisher (only if missing).
    """
    if not isinstance(package_dict, dict):
        return

    source_url = ""
    try:
        if harvest_object is not None and getattr(harvest_object, "source", None) is not None:
            source_url = (harvest_object.source.url or "")
    except Exception:
        source_url = ""

    landing_page = (package_dict.get("landing_page") or "")

    haystack = f"{source_url}\n{landing_page}".lower()
    if "cityofathens" not in haystack:
        return

    if package_dict.get("publisher"):
        return

    package_dict["publisher"] = [{
        "uri": "urn:geonode:group:4",
        "name": "Τμήμα Διαχείρισης Γεωχωρικών Δεδομένων Πόλεως",
        "email": "t.gis@athens.gr",
        "url": "http://gis.cityofathens.gr/"
    }]

# -----------------------------------------------------------------------------
# Download url
# -----------------------------------------------------------------------------

def _normalize_resource_name(v: str) -> str:
    return (str(v).strip() if v is not None else "")


def _is_immediately_downloadable(protocol: str = "") -> bool:
    """
    Protocol-only heuristic:
    Return True if protocol contains a download indication.
    Examples often seen in ISO19139:
      - 'WWW:DOWNLOAD-1.0-http--download'
    """
    p = (protocol or "").strip().lower()
    if not p:
        return False

    # keep it very permissive, as requested
    return ("download" in p) or ("downloadable" in p)


def _extract_online_resources(xml_tree) -> list[dict[str, str]]:
    """
    Extracts online resources from ISO19139:
      - name
      - url
      - protocol
      - description
    """
    if xml_tree is None:
        return []

    nodes = xml_tree.xpath("//gmd:onLine/gmd:CI_OnlineResource", namespaces=NS)
    out: list[dict[str, str]] = []

    def first_non_empty(values: list[Any]) -> str:
        for value in values:
            s = str(value).strip()
            if s:
                return s
        return ""

    for n in nodes:
        url_nodes = n.xpath(".//gmd:linkage/gmd:URL/text()", namespaces=NS)
        protocol_nodes = n.xpath(".//gmd:protocol/*/text() | .//gmd:protocol/text()", namespaces=NS)
        name_nodes = n.xpath(".//gmd:name/*/text() | .//gmd:name/text()", namespaces=NS)
        desc_nodes = n.xpath(".//gmd:description/*/text() | .//gmd:description/text()", namespaces=NS)

        url = _normalize_url(first_non_empty(url_nodes))
        protocol = first_non_empty(protocol_nodes)
        name = _normalize_resource_name(first_non_empty(name_nodes))
        desc = first_non_empty(desc_nodes)

        if not url and not name and not protocol:
            continue

        out.append({
            "name": name,
            "url": url,
            "protocol": protocol,
            "description": desc,
        })

    return out


def apply_download_url_for_direct_downloads(
    package_dict: dict[str, Any],
    xml_tree,
    *,
    overwrite: bool = False,
) -> None:
    """
    For each resource in package_dict["resources"], find its matching ISO onlineResource
    by (name + url). If the protocol indicates DOWNLOAD, set:
      resource["download_url"] = resource["url"]
    """
    if not isinstance(package_dict, dict):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list) or not resources:
        return

    online = _extract_online_resources(xml_tree)
    if not online:
        return

    # Index by (name_lower, url_norm) -> protocol
    index: dict[tuple[str, str], dict[str, str]] = {}
    for o in online:
        n = (o.get("name") or "").strip()
        u = _normalize_url(o.get("url") or "")
        if not n or not u:
            continue
        index[(n.lower(), u)] = o

    for res in resources:
        if not isinstance(res, dict):
            continue

        if not overwrite and res.get("download_url"):
            continue

        res_name = _normalize_resource_name(res.get("name") or "")
        res_url = _normalize_url(res.get("url") or "")
        if not res_name or not res_url:
            continue

        o = index.get((res_name.lower(), res_url))
        if not o:
            continue

        protocol = o.get("protocol") or ""
        if _is_immediately_downloadable(protocol):
            res["download_url"] = res_url


# -----------------------------------------------------------------------------
# Configured WMS layer resource
# -----------------------------------------------------------------------------

def extract_layer_name_from_dataset_identifiers(xml_tree) -> str:
    """
    Extracts the layer local name from ISO19139 dataset identifiers.

    The expected identifier list usually contains the metadata UUID first and
    the layer local name second. We skip UUID-like values and use the first
    non-UUID value, so raster layer names are read from identifier/code instead
    of citation title.
    """
    if xml_tree is None:
        return ""

    values = xml_tree.xpath(
        (
            "//gmd:identificationInfo/gmd:MD_DataIdentification"
            "/gmd:citation/gmd:CI_Citation"
            "/gmd:identifier/gmd:RS_Identifier"
            "/gmd:code/gco:CharacterString/text()"
        ),
        namespaces=NS,
    )

    for value in values:
        layer_name = str(value).strip()
        if layer_name and not _is_uuid_like_layer_identifier(layer_name):
            return layer_name

    return ""


def _is_uuid_like_layer_identifier(value: str) -> bool:
    return bool(_LAYER_IDENTIFIER_UUID_LIKE_RE.match(str(value or "").strip()))


def extract_dataset_identifier_codes(xml_tree) -> list[str]:
    """
    Extracts dataset citation identifier code values from ISO19139 XML.
    """
    if xml_tree is None:
        return []

    values = xml_tree.xpath(
        (
            "//gmd:identificationInfo/gmd:MD_DataIdentification"
            "/gmd:citation/gmd:CI_Citation"
            "/gmd:identifier/gmd:RS_Identifier"
            "/gmd:code/gco:CharacterString/text()"
        ),
        namespaces=NS,
    )

    return [str(value).strip() for value in values if str(value).strip()]


def should_skip_dataset_with_uuid_like_layer_identifier(
    xml_tree,
    harvest_object: Any = None,
) -> bool:
    """
    Returns True when configured to skip records whose dataset identifiers do
    not contain any non-UUID-like layer identifier.

    Enabled only with harvest source config:
      {"skip_dataset_when_no_non_uuid_layer_identifier": true}
    """
    config = get_harvest_source_config(harvest_object)
    if not config.get(SKIP_DATASET_WHEN_NO_NON_UUID_LAYER_IDENTIFIER_CONFIG_KEY):
        return False

    if not str(config.get(LAYER_RESOURCE_BASE_URL_CONFIG_KEY) or "").strip():
        return False

    identifiers = extract_dataset_identifier_codes(xml_tree)
    if not identifiers:
        return False

    has_uuid_like_identifier = any(
        _is_uuid_like_layer_identifier(identifier)
        for identifier in identifiers
    )
    has_non_uuid_identifier = any(
        not _is_uuid_like_layer_identifier(identifier)
        for identifier in identifiers
    )

    return has_uuid_like_identifier and not has_non_uuid_identifier


def _normalize_layer_title_for_match(value: str) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    return re.sub(r"[-_]+", "-", normalized)


def _package_title_values(package_dict: dict[str, Any]) -> list[str]:
    titles = []
    title = str(package_dict.get("title") or "").strip()
    if title:
        titles.append(title)

    title_translated = package_dict.get("title_translated")
    if isinstance(title_translated, dict):
        for value in title_translated.values():
            translated_title = str(value or "").strip()
            if translated_title:
                titles.append(translated_title)

    return titles


def should_skip_dataset_when_title_matches_layer_name(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> bool:
    """
    Returns True when configured to skip CSW records whose title is just the
    WMS layer identifier.

    Enabled only with harvest source config:
      {
        "layer_resource_base_url": "https://.../geoserver/wms#",
        "skip_dataset_when_title_matches_layer_name": true
      }
    """
    if not isinstance(package_dict, dict):
        return False

    config = get_harvest_source_config(harvest_object)
    if not config.get(SKIP_DATASET_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY):
        return False

    if not str(config.get(LAYER_RESOURCE_BASE_URL_CONFIG_KEY) or "").strip():
        return False

    layer_name = extract_layer_name_from_dataset_identifiers(xml_tree)
    if not layer_name:
        return False

    local_layer_name = layer_name.split(":", 1)[-1]
    layer_values = {
        _normalize_layer_title_for_match(layer_name),
        _normalize_layer_title_for_match(local_layer_name),
    }
    layer_values.discard("")

    return any(
        _normalize_layer_title_for_match(title) in layer_values
        for title in _package_title_values(package_dict)
    )


def get_harvest_source_config(harvest_object: Any = None) -> dict[str, Any]:
    """
    Returns harvest_object.source.config as a dict.
    Invalid or missing JSON config is treated as empty.
    """
    raw_config = None
    for path in (("source",), ("job", "source")):
        try:
            current = harvest_object
            for attr in path:
                current = getattr(current, attr, None)
                if current is None:
                    break
            raw_config = getattr(current, "config", None) if current is not None else None
        except Exception:
            raw_config = None

        if raw_config:
            break

    if not raw_config:
        return {}

    if isinstance(raw_config, dict):
        return raw_config

    try:
        parsed = json.loads(raw_config)
    except Exception:
        return {}

    return parsed if isinstance(parsed, dict) else {}


def _resource_value(resource: Any, key: str) -> Any:
    if isinstance(resource, dict):
        return resource.get(key)
    return getattr(resource, key, None)


def _harvest_source_id(harvest_object: Any = None) -> Any:
    for path in (("source",), ("job", "source")):
        try:
            current = harvest_object
            for attr in path:
                current = getattr(current, attr, None)
                if current is None:
                    break
            source_id = getattr(current, "id", None) if current is not None else None
        except Exception:
            source_id = None

        if source_id:
            return source_id

    return None


def _existing_package_resources_for_harvest_object(harvest_object: Any = None) -> list[Any]:
    """
    Returns existing package resources for the package being updated, if known.

    During CSW import, ckanext-harvest may create a fresh HarvestObject for the
    current run. In that case the existing package id can be found via a previous
    HarvestObject with the same guid.
    """
    if harvest_object is None:
        return []

    existing_resources = getattr(harvest_object, "existing_resources", None)
    if isinstance(existing_resources, list):
        return existing_resources

    package = getattr(harvest_object, "package", None)
    if package is not None:
        resources = getattr(package, "resources_all", None) or getattr(package, "resources", None)
        if resources is not None:
            return list(resources)

    package_id = getattr(harvest_object, "package_id", None)

    if not package_id:
        guid = getattr(harvest_object, "guid", None)
        source_id = _harvest_source_id(harvest_object)
        if guid:
            try:
                from ckan import model
                from ckanext.harvest.model import HarvestObject

                query = (
                    model.Session.query(HarvestObject)
                    .filter(HarvestObject.guid == guid)
                    .filter(HarvestObject.package_id.isnot(None))
                )
                if source_id:
                    query = query.filter(HarvestObject.harvest_source_id == source_id)

                previous_object = query.order_by(HarvestObject.gathered.desc()).first()
                package_id = getattr(previous_object, "package_id", None)
            except Exception:
                package_id = None

    if not package_id:
        return []

    try:
        from ckan import model

        package = model.Package.get(package_id)
        if package is None:
            return []
        resources = getattr(package, "resources_all", None) or getattr(package, "resources", None)
        return list(resources or [])
    except Exception:
        return []


def _find_existing_resource_by_url(harvest_object: Any, url: str) -> Any:
    normalized_url = _normalize_url(url)
    if not normalized_url:
        return None

    for resource in _existing_package_resources_for_harvest_object(harvest_object):
        if _resource_value(resource, "state") == "deleted":
            continue
        if _normalize_url(_resource_value(resource, "url") or "") == normalized_url:
            return resource

    return None


def preserve_resource_ids_by_url(
    package_dict: dict[str, Any],
    harvest_object: Any = None,
) -> None:
    """
    Preserves existing resource ids by matching resource URLs.

    If resource_locator_protocol is present, it is used together with the URL.
    This keeps GeoNode WMS/WFS resources distinct when they share the same OWS
    endpoint URL.
    If older resources do not expose resource_locator_protocol, format is used
    as a secondary discriminator.

    Enabled by default. To disable:
      {"preserve_resource_ids_by_url": false}

    This helps CKAN keep resource views attached across re-harvests.
    """
    if not isinstance(package_dict, dict):
        return

    config = get_harvest_source_config(harvest_object)
    if config.get(PRESERVE_RESOURCE_IDS_BY_URL_CONFIG_KEY) is False:
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list) or not resources:
        return

    existing_by_url_protocol: dict[tuple[str, str], Any] = {}
    existing_by_url_format: dict[tuple[str, str], Any] = {}
    existing_by_url: dict[str, list[Any]] = {}
    for existing_resource in _existing_package_resources_for_harvest_object(harvest_object):
        if _resource_value(existing_resource, "state") == "deleted":
            continue

        existing_resource_id = _resource_value(existing_resource, "id")
        normalized_url = _normalize_url(_resource_value(existing_resource, "url") or "")
        if not existing_resource_id or not normalized_url:
            continue

        existing_by_url.setdefault(normalized_url, []).append(existing_resource_id)

        protocol = str(
            _resource_value(existing_resource, "resource_locator_protocol") or ""
        ).strip().upper()
        if protocol:
            existing_by_url_protocol.setdefault(
                (normalized_url, protocol),
                existing_resource_id,
            )

        resource_format = str(_resource_value(existing_resource, "format") or "").strip().upper()
        if resource_format:
            existing_by_url_format.setdefault(
                (normalized_url, resource_format),
                existing_resource_id,
            )

    if not existing_by_url and not existing_by_url_protocol and not existing_by_url_format:
        return

    used_existing_ids: set[Any] = {
        resource.get("id")
        for resource in resources
        if isinstance(resource, dict) and resource.get("id")
    }

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        if resource.get("id"):
            continue

        normalized_url = _normalize_url(resource.get("url") or "")
        if not normalized_url:
            continue

        existing_resource_id = None
        protocol = str(resource.get("resource_locator_protocol") or "").strip().upper()
        if protocol:
            existing_resource_id = existing_by_url_protocol.get(
                (normalized_url, protocol)
            )

        if not existing_resource_id:
            resource_format = str(resource.get("format") or "").strip().upper()
            if resource_format:
                existing_resource_id = existing_by_url_format.get(
                    (normalized_url, resource_format)
                )

        if not existing_resource_id:
            candidates = existing_by_url.get(normalized_url, [])
            if len(candidates) == 1:
                existing_resource_id = candidates[0]

        if not existing_resource_id or existing_resource_id in used_existing_ids:
            continue

        resource["id"] = existing_resource_id
        used_existing_ids.add(existing_resource_id)


def prepend_configured_wms_layer_resource(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> None:
    """
    Prepends a WMS resource based on harvest source config and ISO layer name.

    Config:
      {"layer_resource_base_url": "https://...&layers=workspace:"}

    Final URL:
      layer_resource_base_url + layer local name
    """
    if not isinstance(package_dict, dict):
        return
    if _is_data_service_package(package_dict):
        return

    config = get_harvest_source_config(harvest_object)
    base_url = str(config.get(LAYER_RESOURCE_BASE_URL_CONFIG_KEY) or "").strip()
    if not base_url:
        return

    layer_name = extract_layer_name_from_dataset_identifiers(xml_tree)
    if not layer_name:
        return

    description = f"Προεπισκόπηση συνόλου δεδομένων - {layer_name}"
    translated_name = {
        "el": layer_name,
        "en": layer_name,
    }
    translated_description = {
        "el": description,
        "en": description,
    }
    final_url = f"{base_url}{layer_name}"
    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        resources = []
        package_dict["resources"] = resources

    normalized_final_url = _normalize_url(final_url)
    existing_resource = _find_existing_resource_by_url(harvest_object, final_url)
    existing_resource_id = (
        _resource_value(existing_resource, "id") if existing_resource else None
    )

    for index, resource in enumerate(resources):
        if not isinstance(resource, dict):
            continue
        if _normalize_url(resource.get("url") or "") != normalized_final_url:
            continue

        if existing_resource_id and not resource.get("id"):
            resource["id"] = existing_resource_id
        resource.setdefault("name", layer_name)
        resource["name_translated"] = translated_name
        resource["description_translated"] = translated_description
        resource["format"] = "WMS"
        if index != 0:
            resources.insert(0, resources.pop(index))
        return

    resources.insert(0, {
        **({"id": existing_resource_id} if existing_resource_id else {}),
        "name": layer_name,
        "name_translated": translated_name,
        "description_translated": translated_description,
        "url": final_url,
        "format": "WMS",
    })


def _first_wms_online_resource_layer_name(xml_tree) -> str:
    """
    Returns the first ISO online resource name with protocol OGC:WMS.
    """
    for online_resource in _extract_online_resources(xml_tree):
        protocol = str(online_resource.get("protocol") or "").strip().upper()
        if protocol != "OGC:WMS":
            continue

        layer_name = str(online_resource.get("name") or "").strip()
        if layer_name:
            return layer_name

    return ""


def prepend_wms_preview_resource_from_online_resource(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> None:
    """
    Prepends a WMS preview resource from the first ISO online resource with
    protocol OGC:WMS.

    Config:
      {
        "wms_preview_from_online_resource": true,
        "wms_preview_base_url": "https://.../geoserver/wms#"
      }

    Final URL:
      wms_preview_base_url + online_resource_name
    """
    if not isinstance(package_dict, dict):
        return
    if _is_data_service_package(package_dict):
        return

    config = get_harvest_source_config(harvest_object)
    if not config.get(WMS_PREVIEW_FROM_ONLINE_RESOURCE_CONFIG_KEY):
        return

    base_url = str(config.get(WMS_PREVIEW_BASE_URL_CONFIG_KEY) or "").strip()
    if not base_url:
        return

    layer_name = _first_wms_online_resource_layer_name(xml_tree)
    if not layer_name:
        return

    description = f"Προεπισκόπηση συνόλου δεδομένων - {layer_name}"
    translated_name = {
        "el": layer_name,
        "en": layer_name,
    }
    translated_description = {
        "el": description,
        "en": description,
    }
    final_url = f"{base_url}{layer_name}"
    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        resources = []
        package_dict["resources"] = resources

    normalized_final_url = _normalize_url(final_url)
    existing_resource = _find_existing_resource_by_url(harvest_object, final_url)
    existing_resource_id = _resource_value(existing_resource, "id") if existing_resource else None

    for index, resource in enumerate(resources):
        if not isinstance(resource, dict):
            continue
        if _normalize_url(resource.get("url") or "") != normalized_final_url:
            continue

        if existing_resource_id and not resource.get("id"):
            resource["id"] = existing_resource_id
        resource.setdefault("name", layer_name)
        resource["name_translated"] = translated_name
        resource["description_translated"] = translated_description
        resource["format"] = "WMS"
        if index != 0:
            resources.insert(0, resources.pop(index))
        return

    resources.insert(0, {
        **({"id": existing_resource_id} if existing_resource_id else {}),
        "name": layer_name,
        "name_translated": translated_name,
        "description_translated": translated_description,
        "url": final_url,
        "format": "WMS",
    })


def _wms_online_resource_layer_names(xml_tree) -> list[str]:
    """
    Returns distinct ISO online resource names whose protocol starts with OGC:WMS.
    """
    layer_names = []
    for online_resource in _extract_online_resources(xml_tree):
        protocol = str(online_resource.get("protocol") or "").strip().upper()
        if not protocol.startswith("OGC:WMS"):
            continue

        layer_name = str(online_resource.get("name") or "").strip()
        if layer_name and layer_name not in layer_names:
            layer_names.append(layer_name)

    return layer_names


def _wms_preview_resource(layer_name: str, url: str) -> dict[str, Any]:
    description = f"Προεπισκόπηση συνόλου δεδομένων - {layer_name}"
    return {
        "name": layer_name,
        "name_translated": {
            "el": layer_name,
            "en": layer_name,
        },
        "description_translated": {
            "el": description,
            "en": description,
        },
        "url": url,
        "format": "WMS",
    }


def prepend_wms_preview_resources_from_wms_online_resources(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> None:
    """
    Prepends WMS preview resources from ISO online resources whose protocol
    starts with OGC:WMS.

    Config:
      {
        "wms_preview_from_wms_online_resources": true,
        "wms_preview_base_url": "https://.../geoserver/wms#"
      }

    Final URL:
      wms_preview_base_url + online_resource_name
    """
    if not isinstance(package_dict, dict):
        return
    if _is_data_service_package(package_dict):
        return

    config = get_harvest_source_config(harvest_object)
    if not config.get(WMS_PREVIEW_FROM_WMS_ONLINE_RESOURCES_CONFIG_KEY):
        return

    base_url = str(config.get(WMS_PREVIEW_BASE_URL_CONFIG_KEY) or "").strip()
    if not base_url:
        return

    layer_names = _wms_online_resource_layer_names(xml_tree)
    if not layer_names:
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        resources = []
        package_dict["resources"] = resources

    for position, layer_name in enumerate(layer_names):
        final_url = f"{base_url}{layer_name}"
        _upsert_resource_at_position(
            resources,
            _wms_preview_resource(layer_name, final_url),
            position,
            harvest_object,
        )


def _configured_capabilities_resource(
    *,
    url: str,
    layer_name: str,
    service_name: str,
    protocol: str,
) -> dict[str, Any]:
    return {
        "name": layer_name,
        "name_translated": {
            "el": "%s capabilities document - %s" % (service_name, layer_name),
            "en": "%s capabilities document - %s" % (service_name, layer_name),
        },
        "description_translated": {
            "el": (
                "%s GetCapabilities document που περιγράφει και το layer %s."
                % (service_name, layer_name)
            ),
            "en": (
                "%s GetCapabilities document that includes layer %s."
                % (service_name, layer_name)
            ),
        },
        "url": url,
        "format": "XML",
        "resource_locator_protocol": protocol,
        "access_url": url,
        "download_url": url,
        "mimetype": XML_MIMETYPE,
    }


def _upsert_resource_at_position(
    resources: list[Any],
    resource: dict[str, Any],
    position: int,
    harvest_object: Any = None,
) -> None:
    normalized_url = _normalize_url(resource.get("url") or "")
    if not normalized_url:
        return

    existing_resource = _find_existing_resource_by_url(
        harvest_object,
        resource.get("url") or "",
    )
    existing_resource_id = (
        _resource_value(existing_resource, "id") if existing_resource else None
    )

    for index, current in enumerate(resources):
        if not isinstance(current, dict):
            continue
        if _normalize_url(current.get("url") or "") != normalized_url:
            continue

        if existing_resource_id and not current.get("id"):
            current["id"] = existing_resource_id
        current.update(resource)
        if index != position:
            resources.insert(position, resources.pop(index))
        return

    if existing_resource_id:
        resource = {**resource, "id": existing_resource_id}
    resources.insert(position, resource)


def insert_configured_wms_wfs_capabilities_resources(
    package_dict: dict[str, Any],
    xml_tree,
    harvest_object: Any = None,
) -> None:
    """
    Inserts configured WMS/WFS GetCapabilities resources after the primary WMS
    preview resource for the layer.
    """
    if not isinstance(package_dict, dict):
        return
    if _is_data_service_package(package_dict):
        return

    config = get_harvest_source_config(harvest_object)
    wms_capabilities_url = str(
        config.get(WMS_CAPABILITIES_URL_CONFIG_KEY) or ""
    ).strip()
    wfs_capabilities_url = str(
        config.get(WFS_CAPABILITIES_URL_CONFIG_KEY) or ""
    ).strip()
    if not wms_capabilities_url and not wfs_capabilities_url:
        return

    layer_name = extract_layer_name_from_dataset_identifiers(xml_tree)
    if not layer_name:
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list):
        resources = []
        package_dict["resources"] = resources

    position = 1 if resources else 0
    if wms_capabilities_url:
        _upsert_resource_at_position(
            resources,
            _configured_capabilities_resource(
                url=wms_capabilities_url,
                layer_name=layer_name,
                service_name="WMS",
                protocol="OGC:WMS",
            ),
            position,
            harvest_object,
        )
        position += 1

    if wfs_capabilities_url:
        _upsert_resource_at_position(
            resources,
            _configured_capabilities_resource(
                url=wfs_capabilities_url,
                layer_name=layer_name,
                service_name="WFS",
                protocol="OGC:WFS",
            ),
            position,
            harvest_object,
        )

# -----------------------------------------------------------------------------
# Format
# -----------------------------------------------------------------------------

SAFE_NAME_EXTENSION_FORMATS = {
    "csv",
    "geojson",
    "gml",
    "jpeg",
    "jpg",
    "json",
    "pdf",
    "png",
    "shp",
    "tif",
    "tiff",
    "xls",
    "xlsx",
    "xml",
    "zip",
}


def _format_from_extension(name: str = "", url: str = "") -> str:
    """
    Try to infer a human-friendly resource format from filename extension.
    Returns '' if cannot infer.
    """
    candidate = (name or "").strip()
    if not candidate:
        try:
            candidate = urlparse(url or "").path
        except Exception:
            candidate = ""

    ext = os.path.splitext(candidate)[1].lower().lstrip(".")
    if not ext:
        return ""

    if ext in ("json", "geojson"):
        return "GeoJSON"
    # if ext == "zip":
    #     return "ZIP"
    if ext == "gml":
        return "GML"
    if ext == "csv":
        return "CSV"
    if ext in ("jpg", "jpeg"):
        return "JPEG"
    if ext == "png":
        return "PNG"
    if ext == "pdf":
        return "PDF"

    return ext.upper()


def _looks_like_filename_or_path(value: str) -> bool:
    """
    Returns True only for values that look like filenames/paths, not free text.
    """
    s = (value or "").strip()
    if not s:
        return False

    try:
        parsed = urlparse(s)
        if parsed.scheme and parsed.path:
            s = parsed.path
    except Exception:
        pass

    basename = os.path.basename(s)
    if not basename or "." not in basename:
        return False

    if re.search(r"\s", basename):
        return False

    root, ext = os.path.splitext(basename)
    if not root or not ext:
        return False

    if ext.lower().lstrip(".") not in SAFE_NAME_EXTENSION_FORMATS:
        return False

    return bool(re.match(r"^\.[A-Za-z0-9]{1,10}$", ext))


def _format_from_extension_safe(name: str = "", url: str = "") -> str:
    """
    Safer extension inference.

    Uses the resource name only when it looks like a filename/path. Always allows
    URL path extension inference.
    """
    candidate = (name or "").strip()
    if candidate and _looks_like_filename_or_path(candidate):
        return _format_from_extension(name=candidate, url="")

    try:
        path = urlparse(url or "").path
    except Exception:
        path = ""

    return _format_from_extension(name="", url=path)


def _format_from_url_query(url: str = "") -> str:
    """
    Infer format from common OGC params:
      - service=WMS + format=image/png
      - service=WFS + outputFormat=json, SHAPE-ZIP, gml2, etc
      - request=GetLegendGraphic, GetMap etc (still relies on format=)
    Returns '' if cannot infer.
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
    except Exception:
        return ""

    def q1(key: str) -> str:
        v = qs.get(key, [])
        return (v[0] or "").strip() if v else ""

    service = q1("service").upper()
    fmt = q1("format")
    out_fmt = q1("outputFormat")

    if service == "WMS" and fmt:
        f = fmt.lower()
        if "image/png" in f:
            return "PNG"
        if "image/jpeg" in f or "image/jpg" in f:
            return "JPEG"
        if "application/pdf" in f:
            return "PDF"
        return fmt.upper()

    if service == "WFS" and out_fmt:
        f = out_fmt.lower()

        if "shape-zip" in f:
            return "SHP"

        if f == "json" or "json" in f:
            return "GeoJSON"

        if "csv" in f:
            return "CSV"
        if "excel" in f or "xls" in f:
            return "XLSX"
        if "gml" in f:
            return "GML"

        return out_fmt.upper()

    return ""


def _format_from_protocol(protocol: str = "") -> str:
    p = (protocol or "").strip().upper()
    if p.startswith("OGC:WMS"):
        return "WMS"
    if p.startswith("OGC:WFS"):
        return "WFS"
    return ""


def _infer_resource_format_from_iso_online_resource(
    online: dict[str, str],
    *,
    safe_name_extension: bool = False,
) -> str:
    """
    online: dict with keys name, url, protocol, description
    Returns a best-effort format string.
    """
    name = online.get("name") or ""
    url = online.get("url") or ""
    protocol = online.get("protocol") or ""

    # 1) strongest: file extension in name/url path
    if safe_name_extension:
        f = _format_from_extension_safe(name=name, url=url)
    else:
        f = _format_from_extension(name=name, url=url)
    if f:
        return f

    # 2) OGC query params
    f = _format_from_url_query(url=url)
    if f:
        return f

    # 3) protocol (service type)
    f = _format_from_protocol(protocol=protocol)
    if f:
        return f

    return ""


def apply_resource_format_from_iso19139(
    package_dict: dict,
    xml_tree,
    *,
    overwrite: bool = False,
    harvest_object: Any = None,
) -> None:
    """
    For each resource in package_dict["resources"], match corresponding ISO onlineResource
    by (name + url) and set resource["format"] accordingly.
    """
    if not isinstance(package_dict, dict):
        return

    resources = package_dict.get("resources")
    if not isinstance(resources, list) or not resources:
        return

    online = _extract_online_resources(xml_tree)
    if not online:
        return

    config = get_harvest_source_config(harvest_object)
    safe_name_extension = bool(config.get(SAFE_RESOURCE_FORMAT_INFERENCE_CONFIG_KEY))

    # Prefer exact protocol-aware matching when CKAN resources include
    # resource_locator_protocol. GeoNode may expose WMS/WFS with identical
    # name+url and only the protocol distinguishes them.
    index_by_name_url_protocol: dict[tuple[str, str, str], dict[str, str]] = {}
    index_by_name_url: dict[tuple[str, str], list[dict[str, str]]] = {}
    for o in online:
        n = (o.get("name") or "").strip()
        u = _normalize_url(o.get("url") or "")
        if not n or not u:
            continue

        name_url_key = (n.lower(), u)
        index_by_name_url.setdefault(name_url_key, []).append(o)

        protocol = str(o.get("protocol") or "").strip().upper()
        if protocol:
            index_by_name_url_protocol[(n.lower(), u, protocol)] = o

    for res in resources:
        if not isinstance(res, dict):
            continue

        if not overwrite and res.get("format"):
            continue

        res_name = _normalize_resource_name(res.get("name") or "")
        res_url = _normalize_url(res.get("url") or "")
        if not res_name or not res_url:
            continue

        name_url_key = (res_name.lower(), res_url)
        o = None
        res_protocol = str(res.get("resource_locator_protocol") or "").strip().upper()
        if res_protocol:
            o = index_by_name_url_protocol.get((*name_url_key, res_protocol))

        if not o:
            candidates = index_by_name_url.get(name_url_key, [])
            if len(candidates) == 1:
                o = candidates[0]

        if not o:
            continue

        inferred = _infer_resource_format_from_iso_online_resource(
            o,
            safe_name_extension=safe_name_extension,
        )
        if not inferred:
            continue

        if inferred:
            res["format"] = inferred
