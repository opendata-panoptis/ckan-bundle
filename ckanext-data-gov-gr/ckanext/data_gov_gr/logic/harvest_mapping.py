# ckanext/data_gov_gr/logic/harvest_mapping.py
from __future__ import annotations

from urllib.parse import urlparse, parse_qs
import os

from typing import Any, Iterable

import ckanext.data_gov_gr.helpers as helpers

NS = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "srv": "http://www.isotc211.org/2005/srv",
    "gml": "http://www.opengis.net/gml",
    "gml32": "http://www.opengis.net/gml/3.2",
    "gco": "http://www.isotc211.org/2005/gco",
    "gmx": "http://www.isotc211.org/2005/gmx",
}

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

    for n in nodes:
        url_nodes = n.xpath(".//gmd:linkage/gmd:URL/text()", namespaces=NS)
        protocol_nodes = n.xpath(".//gmd:protocol/*/text() | .//gmd:protocol/text()", namespaces=NS)
        name_nodes = n.xpath(".//gmd:name/*/text() | .//gmd:name/text()", namespaces=NS)
        desc_nodes = n.xpath(".//gmd:description/*/text() | .//gmd:description/text()", namespaces=NS)

        url = _normalize_url(url_nodes[0]) if url_nodes else ""
        protocol = str(protocol_nodes[0]).strip() if protocol_nodes else ""
        name = _normalize_resource_name(name_nodes[0]) if name_nodes else ""
        desc = str(desc_nodes[0]).strip() if desc_nodes else ""

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
# Format
# -----------------------------------------------------------------------------

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
    if p == "OGC:WMS":
        return "WMS"
    if p == "OGC:WFS":
        return "WFS"
    return ""


def _infer_resource_format_from_iso_online_resource(online: dict[str, str]) -> str:
    """
    online: dict with keys name, url, protocol, description
    Returns a best-effort format string.
    """
    name = online.get("name") or ""
    url = online.get("url") or ""
    protocol = online.get("protocol") or ""

    # 1) strongest: file extension in name/url path
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

    # Index by (name_lower, url_norm) -> online dict
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

        if not overwrite and res.get("format"):
            continue

        res_name = _normalize_resource_name(res.get("name") or "")
        res_url = _normalize_url(res.get("url") or "")
        if not res_name or not res_url:
            continue

        o = index.get((res_name.lower(), res_url))
        if not o:
            continue

        inferred = _infer_resource_format_from_iso_online_resource(o)
        if not inferred:
            continue

        if inferred:
            res["format"] = inferred
