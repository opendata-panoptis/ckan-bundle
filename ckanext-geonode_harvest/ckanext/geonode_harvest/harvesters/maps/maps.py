import json
import logging
import re
from html import unescape

from typing import Any

import ckan.logic as logic
import requests
from ckan.plugins import toolkit
from ckanext.data_gov_gr import helpers as data_gov_helpers
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)

# =============================================================================
# Constants / Vocab mappings
# =============================================================================

IANA_MEDIA_TYPES_BASE = "https://www.iana.org/assignments/media-types/"

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

# ISO 19115 / INSPIRE TopicCategory -> EU Publications Office data-theme (codes)
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
    "annually":     "http://publications.europa.eu/resource/authority/frequency/ANNUAL",
    "biannually":   "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2",
    "biennially":   "http://publications.europa.eu/resource/authority/frequency/BIENNIAL",
    "continual":    "http://publications.europa.eu/resource/authority/frequency/CONT",
    "daily":        "http://publications.europa.eu/resource/authority/frequency/DAILY",
    "fortnightly":  "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY",
    "irregular":    "http://publications.europa.eu/resource/authority/frequency/IRREG",
    "monthly":      "http://publications.europa.eu/resource/authority/frequency/MONTHLY",
    "notPlanned":   "http://publications.europa.eu/resource/authority/frequency/NOT_PLANNED",
    "periodic":     "http://publications.europa.eu/resource/authority/frequency/OTHER",
    "quarterly":    "http://publications.europa.eu/resource/authority/frequency/QUARTERLY",
    "semimonthly":  "http://publications.europa.eu/resource/authority/frequency/MONTHLY_2",
    "unknown":      "http://publications.europa.eu/resource/authority/frequency/UNKNOWN",
    "weekly":       "http://publications.europa.eu/resource/authority/frequency/WEEKLY",
    "asNeeded":     "http://publications.europa.eu/resource/authority/frequency/AS_NEEDED",
}

GEONODE_LICENCE_TO_EU_URI = {
    "odbl": "http://publications.europa.eu/resource/authority/licence/ODC_BL",
}

# =============================================================================
# Pure helpers (string/html/tags/ids)
# =============================================================================

def _get_default_tags_from_config(cfg: dict[str, Any]) -> list[str]:
    """
    Supports ONLY:
      "default_tags": ["geo", "Ανοιχτά Δεδομένα"]
    """
    raw = cfg.get("default_tags", [])
    if not isinstance(raw, list):
        return []

    out: list[str] = []
    for x in raw:
        if not isinstance(x, str):
            continue
        t = _clean_tag_string_value(x)
        if t:
            out.append(t)
    return out


def _merge_tags_defaults_first(defaults: list[str], existing: list[str]) -> list[str]:
    """
    Defaults go first, then existing. Dedup is case-insensitive (keeps first occurrence).
    """
    result: list[str] = []
    seen: set[str] = set()

    for t in defaults + existing:
        tt = (t or "").strip()
        if not tt:
            continue
        key = tt.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(tt)

    return result


def _clean_tag_string_value(s):
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"[^0-9A-Za-zΑ-Ωα-ωΆΈΉΊΌΎΏάέήίόύώϊϋΐΰ ._-]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _strip_html(html):
    if not html:
        return ""
    txt = re.sub(r"<[^>]+>", "", html)
    return unescape(txt).strip()


def _safe_notes_el(obj):
    raw = (obj.get("raw_abstract") or "").strip()
    if not raw or raw.lower() == "no abstract provided":
        sup = (obj.get("raw_supplemental_information") or "").strip()
        if sup and sup != "Δεν παρέχεται καμία πληροφορία":
            return sup
        return "Δεν παρέχεται περιγραφή"
    if "<" in raw and ">" in raw:
        return _strip_html(raw)
    return raw


def _person_label(u: dict) -> str:
    if not u:
        return ""
    fn = (u.get("first_name") or "").strip()
    ln = (u.get("last_name") or "").strip()
    if (fn or ln):
        return (fn + " " + ln).strip()
    return (u.get("username") or "").strip()


def _map_topiccategory_to_theme_values(topiccategory: str, use_uri: bool = False) -> list[str]:
    tc = (topiccategory or "").strip()
    codes = TOPICCATEGORY_TO_EU_THEME.get(tc, [])
    if not use_uri:
        return codes
    return [EU_THEME_URI[c] for c in codes if c in EU_THEME_URI]


def _map_maintenance_frequency_to_eu_uri(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    return ISO_MAINTFREQ_TO_EU_FREQ_URI.get(v, "")


def map_geonode_licence_to_eu_uri(value: str | None) -> str | None:
    if not value:
        return None

    v = value.strip().lower()

    if v == "not_specified":
        return None

    return GEONODE_LICENCE_TO_EU_URI.get(v)


def to_iana_mimetype(value: str | None) -> str | None:
    """
    Input:  'image/jpeg' OR already-iana 'https://www.iana.org/assignments/media-types/image/jpeg'
    Output: always IANA URI, or None
    """
    if not value:
        return None
    v = value.strip()

    if v.startswith(IANA_MEDIA_TYPES_BASE):
        return v

    v = v.split(";", 1)[0].strip()
    v = v.lower()

    if "/" not in v or v.startswith("/") or v.endswith("/"):
        return None

    return f"{IANA_MEDIA_TYPES_BASE}{v}"


def _extras_put(extras, key, value):
    """Store extras only if value is meaningful. Always stringifies non-str."""
    if value is None:
        return
    if isinstance(value, str):
        v = value.strip()
        if not v or v.lower() in ("not_specified", "none", "null"):
            return
        extras.append({"key": key, "value": v})
        return

    try:
        extras.append({"key": key, "value": json.dumps(value, ensure_ascii=False)})
    except Exception:
        extras.append({"key": key, "value": str(value)})


# =============================================================================
# Harvester: GeoNode Maps
# =============================================================================

class GeonodeMapsHarvester(HarvesterBase):

    # -------------------------------------------------------------------------
    # Harvester interface
    # -------------------------------------------------------------------------

    def info(self):
        return {
            "name": "geonode_maps",
            "title": "GeoNode Maps (API v2)",
            "description": "Harvest GeoNode maps via harvest source URL (e.g. /api/v2/maps)",
        }

    # -------------------------------------------------------------------------
    # CKAN context / upsert
    # -------------------------------------------------------------------------

    def _site_user_context(self):
        site_user = toolkit.get_action("get_site_user")({"ignore_auth": True}, {})
        return {"user": site_user["name"]}

    def _upsert_package(self, package_dict):
        """
        Create or update dataset by name, running as site user (avoids auth-wrapper errors).
        """
        context = self._site_user_context()
        name = package_dict["name"]

        try:
            existing = toolkit.get_action("package_show")(context.copy(), {"id": name})
            package_dict["id"] = existing["id"]

            # Resource de-dup by URL (keep ids stable across updates)
            existing_by_url = {r.get("url"): r.get("id") for r in existing.get("resources", [])}
            for r in package_dict.get("resources", []) or []:
                rid = existing_by_url.get(r.get("url"))
                if rid:
                    r["id"] = rid

            return toolkit.get_action("package_update")(context.copy(), package_dict)

        except (logic.NotFound, toolkit.ObjectNotFound):
            return toolkit.get_action("package_create")(context.copy(), package_dict)

    def _ensure_webpage_view(self, context, resource_id: str, title: str, url: str) -> None:
        """
        Δημιουργεί webpage view (ckanext-views) αν δεν υπάρχει ήδη.
        Dedup με βάση view_type=webpage_view και ίδιο page_url.
        """
        if not (resource_id and url):
            return

        try:
            existing_views = toolkit.get_action("resource_view_list")(context, {"id": resource_id}) or []
        except Exception:
            existing_views = []

        target = url.strip()

        for v in existing_views:
            if v.get("view_type") == "webpage_view" and (v.get("page_url") or "").strip() == target:
                return  # already exists

        # webpage_view expects page_url as a top-level field (plugin schema), not url/config
        view_dict = {
            "resource_id": resource_id,
            "title": title,
            "view_type": "webpage_view",
            "page_url": target,
        }

        toolkit.get_action("resource_view_create")(context, view_dict)

    # -------------------------------------------------------------------------
    # Source config / org / session
    # -------------------------------------------------------------------------

    def _get_source_owner_org(self, harvest_source_id):
        src = toolkit.get_action("package_show")(
            {"ignore_auth": True},
            {"id": harvest_source_id},
        )
        return src.get("owner_org")

    def _get_source_config(self, harvest_source):
        try:
            return json.loads(harvest_source.config or "{}")
        except Exception:
            return {}

    def _is_cityofathens_source(self, harvest_source) -> bool:
        """
        True μόνο αν το harvest_source.url περιέχει 'cityofathens' (case-insensitive).
        Δεν κοιτάμε config ή άλλα πεδία.
        """
        u = (getattr(harvest_source, "url", None) or "")
        return "cityofathens" in str(u).lower()

    def _session(self):
        s = requests.Session()
        s.headers.update({"Accept": "application/json"})
        return s

    # -------------------------------------------------------------------------
    # GeoNode URL helpers
    # -------------------------------------------------------------------------

    def _get_maps_list_base(self, harvest_source):
        """
        Αναμένουμε ότι στο UI URL θα βάλεις:
          http://gis.cityofathens.gr/api/v2/maps
        """
        return (harvest_source.url or "").rstrip("/")

    def _get_api_base_from_maps_url(self, maps_url):
        """
        Από .../api/v2/maps -> .../api/v2
        """
        u = maps_url.rstrip("/")
        if u.endswith("/maps"):
            return u[: -len("/maps")]
        return u.rsplit("/", 1)[0]

    def _iter_map_list_items(self, sess, maps_base):
        """
        Generator που φέρνει όλα τα list-items (με pagination).
        """
        url = f"{maps_base}?format=json&page=1"
        while url:
            r = sess.get(url, timeout=60)
            r.raise_for_status()
            payload = r.json()

            for m in payload.get("maps", []) or []:
                yield m

            url = (payload.get("links") or {}).get("next")

    def _fetch_map_detail(self, sess, api_base, pk):
        detail_url = f"{api_base}/maps/{pk}?format=json"
        r = sess.get(detail_url, timeout=60)
        r.raise_for_status()
        payload = r.json()
        return payload.get("map") or payload

    # -------------------------------------------------------------------------
    # gather/fetch
    # -------------------------------------------------------------------------

    def gather_stage(self, harvest_job):
        source = harvest_job.source
        cfg = self._get_source_config(source)
        only_published = cfg.get("only_published", True)

        maps_base = self._get_maps_list_base(source)
        if not maps_base:
            raise Exception("Harvest source URL is empty. Set it to .../api/v2/maps")

        sess = self._session()

        object_ids = []
        for m in self._iter_map_list_items(sess, maps_base):
            if only_published and not (m.get("is_published") and m.get("is_approved")):
                continue

            guid = m.get("uuid") or f"pk:{m.get('pk')}"

            log.info(
                "Got identifier %s from GeoNode maps (job_id=%s, source_id=%s)",
                guid,
                getattr(harvest_job, "id", None),
                getattr(source, "id", None),
            )

            ho = HarvestObject(guid=guid, job=harvest_job)
            ho.content = json.dumps(m)  # list-item (για pk)
            ho.save()
            object_ids.append(ho.id)

        log.info(
            "GeonodeMapsHarvester gathered %s objects (job_id=%s, source_id=%s)",
            len(object_ids),
            getattr(harvest_job, "id", None),
            getattr(source, "id", None),
        )
        return object_ids

    def fetch_stage(self, harvest_object):
        """
        Fetches full map JSON for the HarvestObject (detail endpoint).
        Note: 'only_published' filtering is applied in gather_stage, so no need here.
        """
        maps_base = self._get_maps_list_base(harvest_object.source)
        api_base = self._get_api_base_from_maps_url(maps_base)
        sess = self._session()

        list_item = json.loads(harvest_object.content or "{}")
        pk = list_item.get("pk")
        if not pk:
            return True

        m = self._fetch_map_detail(sess, api_base, pk)
        harvest_object.content = json.dumps(m)
        harvest_object.save()
        return True

    # -------------------------------------------------------------------------
    # import mapping builders
    # -------------------------------------------------------------------------

    def _build_identity(self, m):
        uuid = m.get("uuid")
        pk = m.get("pk")
        title = (m.get("title") or uuid or str(pk)).strip()

        if uuid:
            name = f"geonode-map-{uuid}"
        else:
            name = f"geonode-map-pk-{pk}"
        name = name.lower()

        title_translated = {"el": title, "en": title}

        notes_el = _safe_notes_el(m)
        notes_translated = {"el": notes_el, "en": notes_el}

        return uuid, pk, name, title_translated, notes_translated

    def _build_tags(self, m) -> list[str]:
        kw_names: list[str] = []
        for kw in (m.get("keywords") or []):
            nm = _clean_tag_string_value((kw or {}).get("name") or "")
            if nm:
                kw_names.append(nm)
        return kw_names

    def _build_agents(self, m, *, is_cityofathens_source: bool = False):
        poc = m.get("poc") or {}
        owner = m.get("owner") or {}
        meta_author = m.get("metadata_author") or {}

        contact_name = _person_label(poc)
        creator_name = _person_label(meta_author) or _person_label(owner)

        contact = []
        if contact_name:
            contact.append({
                "name": contact_name,
                "email": "",
                "url": m.get("detail_url") or "",
                "uri": f"urn:geonode:user:{poc.get('pk')}" if poc.get("pk") else "",
            })

        creator = []
        if creator_name:
            creator.append({
                "name": creator_name,
                "email": "",
                "url": "",
                "uri": f"urn:geonode:user:{meta_author.get('pk')}" if meta_author.get("pk") else "",
                "type": "",
                "identifier": "",
                "description": "",
            })

        group = m.get("group") or {}
        publisher = []
        if group.get("name"):
            publ = {
                "name": group.get("name"),
                "email": "",
                "url": "",
                "uri": f"urn:geonode:group:{group.get('pk')}" if group.get("pk") else "",
                "type": "",
                "identifier": "",
                "description": "",
            }

            # ΜΟΝΟ για City of Athens sources + group pk=4
            if is_cityofathens_source and str(group.get("pk")) == "4":
                publ["name"] = "Τμήμα Διαχείρισης Γεωχωρικών Δεδομένων Πόλεως"
                publ["email"] = "t.gis@athens.gr"
                publ["url"] = "http://gis.cityofathens.gr/"
                publ["description"] = "Τμήμα Διαχείρισης Γεωχωρικών Δεδομένων Πόλεως"

            publisher.append(publ)

        return contact, creator, publisher

    def _build_resources(self, m, landing):
        """
        Maps: κρατάμε 1 HTML resource (landing) + 1 thumbnail (PNG).
        Το embed_url μπαίνει στη περιγραφή του landing resource (αν υπάρχει).
        """
        embed_url = (m.get("embed_url") or "").strip()
        thumb_url = (m.get("thumbnail_url") or "").strip()

        licence_uri = map_geonode_licence_to_eu_uri((m.get("license") or {}).get("identifier"))

        resources = []

        # 1) Landing resource (single HTML)
        if landing:
            embed_note_el = f"Διαθέσιμο embed URL: {embed_url}" if embed_url else ""
            embed_note_en = f"Available embed URL: {embed_url}" if embed_url else ""

            res = {
                "url": landing,
                "access_url": landing,
                "download_url": "",
                "format": "HTML",
                "name_translated": {"el": "Σελίδα χάρτη", "en": "Map page"},
                "description_translated": {"el": embed_note_el, "en": embed_note_en},
                "mimetype": to_iana_mimetype("text/html"),
            }
            if licence_uri:
                res["license"] = licence_uri

            resources.append(res)

        # 2) Thumbnail resource (PNG)
        if thumb_url:
            res = {
                "url": thumb_url,
                "access_url": landing or thumb_url,
                "download_url": thumb_url,
                "format": "PNG",
                "name_translated": {"el": "Μικρογραφία", "en": "Thumbnail"},
                "description_translated": {"el": "", "en": ""},
                "mimetype": to_iana_mimetype("image/png"),
            }
            if licence_uri:
                res["license"] = licence_uri
            resources.append(res)

        return resources

    def _build_spatial_temporal(self, m):
        spatial_coverage = []
        poly = m.get("ll_bbox_polygon") or m.get("bbox_polygon")
        if poly and isinstance(poly, dict) and poly.get("type") == "Polygon":
            coords = poly.get("coordinates") or []
            world = [[[-180.0, -90.0], [-180.0, 90.0], [180.0, 90.0], [180.0, -90.0], [-180.0, -90.0]]]
            if coords != world:
                spatial_coverage = [{"geom": json.dumps(poly)}]

        temporal_coverage = []
        ts = m.get("temporal_extent_start")
        te = m.get("temporal_extent_end")
        if ts or te:
            temporal_coverage = [{"start": ts, "end": te}]

        return spatial_coverage, temporal_coverage

    def _build_extras_and_provenance(self, m, uuid, pk):
        extras = [
            {"key": "geonode_uuid", "value": uuid},
            {"key": "geonode_pk", "value": pk},
            {"key": "geonode_last_updated", "value": m.get("last_updated")},
            {"key": "geonode_srid", "value": m.get("srid")},
            {"key": "geonode_projection", "value": m.get("projection")},
            {"key": "geonode_zoom", "value": str(m.get("zoom") or "")},
            {"key": "geonode_center_x", "value": str(m.get("center_x") or "")},
            {"key": "geonode_center_y", "value": str(m.get("center_y") or "")},
            {"key": "geonode_urlsuffix", "value": (m.get("urlsuffix") or "")},
            {"key": "geonode_featuredurl", "value": (m.get("featuredurl") or "")},
            {"key": "geonode_group", "value": (m.get("group") or {}).get("name")},
            {"key": "geonode_spatial_representation_type",
             "value": ((m.get("spatial_representation_type") or {}).get("identifier") or "")},
        ]

        USED_MAP_KEYS = {
            "pk", "uuid",
            "title", "abstract", "raw_abstract",
            "supplemental_information", "raw_supplemental_information",
            "data_quality_statement", "raw_data_quality_statement",
            "detail_url", "embed_url", "thumbnail_url",
            "keywords",
            "category", "maintenance_frequency",
            "temporal_extent_start", "temporal_extent_end",
            "ll_bbox_polygon", "bbox_polygon", "srid",
            "license",
            "language",
            "owner", "poc", "metadata_author", "group",
            "created", "last_updated",
            "zoom", "projection", "center_x", "center_y", "urlsuffix", "featuredurl",
            "spatial_representation_type",
            "featured", "is_published", "is_approved",
            "popular_count", "share_count", "rating",
            "resource_type", "polymorphic_ctype_id",
            "date", "date_type", "edition",
            "purpose", "raw_purpose",
            "constraints_other", "raw_constraints_other",
            "restriction_code_type",
            "alternate", "doi", "attribution",
            "regions",
            "metadata_only", "processed",
            "perms",
        }

        unmapped = {}
        for k, v in (m or {}).items():
            if k in USED_MAP_KEYS:
                continue
            unmapped[k] = v

        _extras_put(extras, "geonode_provenance_unmapped", unmapped)

        # Αν ποτέ θες full raw (συνήθως όχι):
        # _extras_put(extras, "geonode_provenance_raw", m)

        return extras

    def _build_theme_and_frequency(self, m, extras):
        topiccategory = (m.get("category") or {}).get("identifier")
        use_theme_uri = True
        theme = _map_topiccategory_to_theme_values(topiccategory, use_uri=use_theme_uri)

        extras.append({"key": "geonode_topiccategory", "value": topiccategory})
        extras.append({
            "key": "geonode_topiccategory_uri",
            "value": f"http://inspire.ec.europa.eu/metadata-codelist/TopicCategory/{topiccategory}"
            if topiccategory else ""
        })

        iso_freq = m.get("maintenance_frequency")
        eu_freq_uri = _map_maintenance_frequency_to_eu_uri(iso_freq)

        extras.append({"key": "geonode_maintenance_frequency_raw", "value": iso_freq or ""})
        if eu_freq_uri:
            extras.append({"key": "geonode_maintenance_frequency_eu_uri", "value": eu_freq_uri})

        return theme, eu_freq_uri

    def _get_legislation_value(self):
        try:
            legislation_value = data_gov_helpers.get_config_value(
                'ckanext.data_gov_gr.dataset.legislation.open',
                'https://eur-lex.europa.eu/eli/dir/2019/1024/oj/eng'
            )
            if isinstance(legislation_value, str):
                legislation_value = legislation_value.strip()
        except Exception:
            legislation_value = 'https://eur-lex.europa.eu/eli/dir/2019/1024/oj/eng'
        return legislation_value

    # -------------------------------------------------------------------------
    # import_stage
    # -------------------------------------------------------------------------

    def import_stage(self, harvest_object):
        if not harvest_object.content:
            return False

        m = json.loads(harvest_object.content)
        owner_org = self._get_source_owner_org(harvest_object.source.id)

        uuid, pk, name, title_translated, notes_translated = self._build_identity(m)

        cfg = self._get_source_config(harvest_object.source)
        default_tags = _get_default_tags_from_config(cfg)

        keyword_tags = self._build_tags(m)
        all_tags = _merge_tags_defaults_first(default_tags, keyword_tags)
        tag_string = ", ".join(all_tags)

        landing = (m.get("detail_url") or "").strip() or None
        embed_url = (m.get("embed_url") or "").strip() or None

        is_cityofathens_source = self._is_cityofathens_source(harvest_object.source)
        contact, creator, publisher = self._build_agents(
            m,
            is_cityofathens_source=is_cityofathens_source,
        )
        resources = self._build_resources(m, landing)

        spatial_coverage, temporal_coverage = self._build_spatial_temporal(m)
        extras = self._build_extras_and_provenance(m, uuid, pk)

        theme, eu_freq_uri = self._build_theme_and_frequency(m, extras)

        dcat_type = "http://publications.europa.eu/resource/authority/dataset-type/GEOSPATIAL"
        access_rights = "http://publications.europa.eu/resource/authority/access-right/PUBLIC"
        legislation_value = self._get_legislation_value()

        package_dict = {
            "name": name,
            "owner_org": owner_org,
            "title_translated": title_translated,
            "notes_translated": notes_translated,
            "tag_string": tag_string,
            "url": landing,
            "landing_page": [landing] if landing else [],
            "access_rights": access_rights,
            "dcat_type": dcat_type,
            "spatial_coverage": spatial_coverage,
            "temporal_coverage": temporal_coverage,
            "theme": theme,
            "publisher": publisher,
            "creator": creator,
            "contact": contact,
            "resources": resources,
            "extras": extras,
        }

        if eu_freq_uri:
            package_dict["frequency"] = eu_freq_uri

        if legislation_value:
            package_dict["applicable_legislation"] = [legislation_value]

        package_dict["language_options"] = ["http://publications.europa.eu/resource/authority/language/ELL"]

        # config flags (default: create views)
        cfg = self._get_source_config(harvest_object.source)
        create_webpage_view = cfg.get("create_webpage_view", True)
        webpage_view_title = (cfg.get("webpage_view_title") or "Ενσωμάτωση χάρτη").strip()

        try:
            result = self._upsert_package(package_dict)
            harvest_object.package_id = result["id"]
            harvest_object.current = True
            harvest_object.save()

            # Auto-create webpage view on the landing HTML resource
            if create_webpage_view and embed_url and landing:
                context = self._site_user_context()

                # find the landing resource in the resulting package
                landing_res_id = None
                for r in (result.get("resources") or []):
                    if (r.get("url") or "").strip() == landing:
                        landing_res_id = r.get("id")
                        break

                if landing_res_id:
                    try:
                        self._ensure_webpage_view(context, landing_res_id, webpage_view_title, embed_url)
                    except toolkit.ValidationError:
                        # View creation is optional; don't fail the whole harvest on a view schema issue
                        log.exception(
                            "Skipping webpage view creation due to ValidationError (resource_id=%s, url=%s)",
                            landing_res_id,
                            embed_url,
                        )
                    except Exception:
                        log.exception(
                            "Skipping webpage view creation due to unexpected error (resource_id=%s, url=%s)",
                            landing_res_id,
                            embed_url,
                        )

            return True

        except Exception as e:
            log.exception("Import failed for %s: %s", harvest_object.guid, e)
            harvest_object.current = False
            harvest_object.save()
            return False