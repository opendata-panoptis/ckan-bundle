# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from hashlib import sha1
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import ckan.plugins.toolkit as toolkit
import requests
from ckan import model
from ckan.model import Session
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from lxml import etree

from ckanext.data_gov_gr.logic.harvest_mapping import (
    apply_default_dataset_fields_from_config,
    apply_default_resource_fields_from_config,
    ensure_applicable_legislation,
    preserve_resource_ids_by_url,
)

log = logging.getLogger(__name__)


WMS_NS = {
    "wms": "http://www.opengis.net/wms",
    "xlink": "http://www.w3.org/1999/xlink",
}

DATASET_NAME_PREFIX_CONFIG_KEY = "dataset_name_prefix_from_layer_name"
LEGACY_DATASET_NAME_PREFIX_CONFIG_KEY = "dataset_name_prefix_from_file_identifier"
DATASET_NAME_MAX_LENGTH_CONFIG_KEY = "dataset_name_max_length"
WMS_PREVIEW_BASE_URL_CONFIG_KEY = "wms_preview_base_url"
WMS_PREVIEW_WORKSPACE_IN_PATH_CONFIG_KEY = "wms_preview_workspace_in_path"
WMS_PREVIEW_RESOURCE_URLS_USE_DATASET_URL_CONFIG_KEY = (
    "wms_preview_resource_urls_use_dataset_url"
)
WMS_CAPABILITIES_URL_CONFIG_KEY = "wms_capabilities_url"
WMS_GETMAP_BASE_URL_CONFIG_KEY = "wms_getmap_base_url"
WMS_GETMAP_BASE_URL_PRESERVE_QUERY_CONFIG_KEY = "wms_getmap_base_url_preserve_query"
WMS_GETMAP_RESOURCES_CONFIG_KEY = "wms_getmap_resources"
WFS_CAPABILITIES_URL_CONFIG_KEY = "wfs_capabilities_url"
WFS_CAPABILITIES_FILE_CONFIG_KEY = "wfs_capabilities_file"
WFS_GETFEATURE_BASE_URL_CONFIG_KEY = "wfs_getfeature_base_url"
WFS_DOWNLOAD_RESOURCES_CONFIG_KEY = "wfs_download_resources"
WFS_LAYER_NAME_PREFIX_CONFIG_KEY = "wfs_layer_name_prefix"
DISABLE_SSL_VERIFICATION_CONFIG_KEY = "disable_ssl_verification"
SKIP_WFS_CAPABILITIES_RESOURCE_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY = (
    "skip_wfs_capabilities_resource_when_layer_missing_from_wfs_capabilities"
)
SKIP_WMS_GETMAP_RESOURCES_WHEN_LAYER_PRESENT_IN_WFS_CAPABILITIES_CONFIG_KEY = (
    "skip_wms_getmap_resources_when_layer_present_in_wfs_capabilities"
)
DEFAULT_THEME_CONFIG_KEY = "default_theme"
GATHER_LOG_EVERY_CONFIG_KEY = "gather_log_every"
INCLUDE_LAYER_NAME_KEYWORDS_CONFIG_KEY = "include_layer_name_keywords"
SKIP_KEYWORDS_MATCHING_CONFIG_KEY = "skip_keywords_matching"
DEFAULT_TAGS_CONFIG_KEY = "default_tags"
SKIP_DATASET_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY = (
    "skip_dataset_when_title_matches_layer_name"
)
INCLUDE_ONLY_DATASETS_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY = (
    "include_only_datasets_when_title_matches_layer_name"
)
TITLE_PREFIX_FOR_LAYER_NAME_TITLES_CONFIG_KEY = "title_prefix_for_layer_name_titles"
SKIP_DATASET_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY = (
    "skip_dataset_when_layer_missing_from_wfs_capabilities"
)
INCLUDE_ONLY_DATASETS_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY = (
    "include_only_datasets_when_layer_missing_from_wfs_capabilities"
)
DEFAULT_TIMEOUT = 60
DEFAULT_DATASET_NAME_MAX_LENGTH = 100

PUBLIC_ACCESS_RIGHTS = "http://publications.europa.eu/resource/authority/access-right/PUBLIC"
GEOSPATIAL_DCAT_TYPE = "http://publications.europa.eu/resource/authority/dataset-type/GEOSPATIAL"
XML_MIMETYPE = "https://www.iana.org/assignments/media-types/application/xml"
PNG_MIMETYPE = "https://www.iana.org/assignments/media-types/image/png"


def _text(element, xpath: str) -> str:
    values = element.xpath(xpath, namespaces=WMS_NS)
    for value in values:
        text = str(value).strip()
        if text:
            return text
    return ""


def normalize_ckan_name(value: str) -> str:
    if not value:
        return ""

    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    slug = re.sub(r"[^a-z0-9_-]+", "-", lowered)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug.strip("-_")


def normalize_ckan_tag(value: str) -> str:
    if not value:
        return ""

    normalized = unicodedata.normalize("NFKC", str(value))
    lowered = normalized.strip().lower().replace(":", "-")
    slug = re.sub(r"[^\w-]+", "-", lowered, flags=re.UNICODE)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug.strip("-_")


def _translated(value: str, fallback: str = "") -> dict[str, str]:
    text = (value or fallback or "").strip()
    return {
        "el": text,
        "en": text,
    }


def _bbox_geojson(bbox: dict[str, float]) -> str:
    west = bbox["west"]
    east = bbox["east"]
    south = bbox["south"]
    north = bbox["north"]
    return json.dumps({
        "type": "Polygon",
        "coordinates": [[
            [west, south],
            [west, north],
            [east, north],
            [east, south],
            [west, south],
        ]],
    })


def _centroid_geojson(bbox: dict[str, float]) -> str:
    west = bbox["west"]
    east = bbox["east"]
    south = bbox["south"]
    north = bbox["north"]
    return json.dumps({
        "type": "Point",
        "coordinates": [
            (west + east) / 2.0,
            (south + north) / 2.0,
        ],
    })


def _parse_float(value: str) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_ex_geographic_bbox(layer) -> dict[str, float] | None:
    bbox_el = layer.find("wms:EX_GeographicBoundingBox", namespaces=WMS_NS)
    if bbox_el is None:
        return None

    values = {
        "west": _parse_float(_text(bbox_el, "./wms:westBoundLongitude/text()")),
        "east": _parse_float(_text(bbox_el, "./wms:eastBoundLongitude/text()")),
        "south": _parse_float(_text(bbox_el, "./wms:southBoundLatitude/text()")),
        "north": _parse_float(_text(bbox_el, "./wms:northBoundLatitude/text()")),
    }
    if any(value is None for value in values.values()):
        return None

    return values


def _layer_crs_values(layer) -> list[str]:
    crs_values = []
    for current in layer.xpath("ancestor-or-self::wms:Layer", namespaces=WMS_NS):
        for value in current.xpath("./wms:CRS/text()", namespaces=WMS_NS):
            value = str(value).strip()
            if value and value not in crs_values:
                crs_values.append(value)
    return crs_values


def parse_wms_capabilities(xml_content: bytes | str) -> dict[str, Any]:
    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=True)
    if isinstance(xml_content, str):
        xml_content = xml_content.encode("utf-8")

    tree = etree.fromstring(xml_content, parser=parser)
    service = tree.find("wms:Service", namespaces=WMS_NS)
    service_title = _text(service, "./wms:Title/text()") if service is not None else ""

    layers = []
    for layer in tree.xpath("//wms:Layer[wms:Name]", namespaces=WMS_NS):
        name = _text(layer, "./wms:Name/text()")
        if not name:
            continue

        keywords = [
            str(value).strip()
            for value in layer.xpath(
                "./wms:KeywordList/wms:Keyword/text()",
                namespaces=WMS_NS,
            )
            if str(value).strip()
        ]
        crs = _layer_crs_values(layer)
        legend_urls = [
            str(value).strip()
            for value in layer.xpath(
                "./wms:Style/wms:LegendURL/wms:OnlineResource/@xlink:href",
                namespaces=WMS_NS,
            )
            if str(value).strip()
        ]

        layers.append({
            "name": name,
            "title": _text(layer, "./wms:Title/text()") or name,
            "abstract": _text(layer, "./wms:Abstract/text()"),
            "keywords": keywords,
            "crs": crs,
            "bbox": _parse_ex_geographic_bbox(layer),
            "legend_url": legend_urls[0] if legend_urls else "",
        })

    return {
        "service_title": service_title,
        "layers": layers,
    }


def parse_wfs_capabilities(xml_content: bytes | str) -> dict[str, Any]:
    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=True)
    if isinstance(xml_content, str):
        xml_content = xml_content.encode("utf-8")

    tree = etree.fromstring(xml_content, parser=parser)
    names = tree.xpath(
        "//*[local-name()='FeatureType']/*[local-name()='Name']/text()"
    )
    getfeature_urls = tree.xpath(
        "//*[local-name()='Operation' and @name='GetFeature']"
        "//*[local-name()='HTTP']/*[local-name()='Get']"
        "/@*[local-name()='href']"
    )
    output_formats = tree.xpath(
        "//*[local-name()='Operation' and @name='GetFeature']"
        "//*[local-name()='Parameter' and @name='outputFormat']"
        "//*[local-name()='Value']/text()"
    )

    return {
        "version": str(tree.get("version") or "").strip() or "2.0.0",
        "layer_names": {str(name).strip() for name in names if str(name).strip()},
        "getfeature_base_url": (
            str(getfeature_urls[0]).strip() if getfeature_urls else ""
        ),
        "output_formats": {
            str(output_format).strip()
            for output_format in output_formats
            if str(output_format).strip()
        },
    }


def parse_wfs_capabilities_layer_names(xml_content: bytes | str) -> set[str]:
    return parse_wfs_capabilities(xml_content)["layer_names"]


class WmsCapabilitiesHarvester(HarvesterBase):
    def info(self):
        return {
            "name": "wms_capabilities_harvester",
            "title": "WMS Capabilities Harvester",
            "description": (
                "Creates one CKAN dataset per named WMS layer from a "
                "GetCapabilities document."
            ),
            "form_config_interface": "Text",
            "show_config": False,
        }

    def validate_config(self, source_config):
        if not source_config:
            return source_config

        try:
            config = json.loads(source_config)
        except ValueError as e:
            raise ValueError("WMS harvester config must be valid JSON: %s" % e)

        if not isinstance(config, dict):
            raise ValueError("WMS harvester config must be a JSON object")

        return source_config

    def gather_stage(self, harvest_job):
        log.info("WMS capabilities gather started for source: %s", harvest_job.source.url)
        config = self._config(harvest_job)
        capabilities_xml = self._load_capabilities(harvest_job, config)
        if not capabilities_xml:
            return []

        try:
            started_at = time.monotonic()
            parsed = parse_wms_capabilities(capabilities_xml)
            log.info(
                "Parsed WMS capabilities: %d layers in %.2fs",
                len(parsed["layers"]),
                time.monotonic() - started_at,
            )
        except Exception as e:
            self._save_gather_error("Could not parse WMS capabilities: %s" % e, harvest_job)
            log.exception("Could not parse WMS capabilities")
            return []

        prefix = self._dataset_name_prefix(config)
        log_every = self._gather_log_every(config)
        skip_title_matches_layer_name = bool(
            config.get(SKIP_DATASET_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY)
        )
        include_only_title_matches_layer_name = bool(
            config.get(
                INCLUDE_ONLY_DATASETS_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY
            )
        )
        if include_only_title_matches_layer_name and skip_title_matches_layer_name:
            log.warning(
                "Both %s and %s are enabled; include-only title match mode "
                "takes precedence",
                INCLUDE_ONLY_DATASETS_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY,
                SKIP_DATASET_WHEN_TITLE_MATCHES_LAYER_NAME_CONFIG_KEY,
            )
        wfs_info = self._wfs_info_for_gather(config, harvest_job)
        if wfs_info is None:
            return []
        skip_layers_missing_from_wfs = bool(
            config.get(
                SKIP_DATASET_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY
            )
        )
        include_only_layers_missing_from_wfs = bool(
            config.get(
                INCLUDE_ONLY_DATASETS_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY
            )
        )
        if include_only_layers_missing_from_wfs and skip_layers_missing_from_wfs:
            log.warning(
                "Both %s and %s are enabled; include-only WFS missing mode "
                "takes precedence",
                INCLUDE_ONLY_DATASETS_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY,
                SKIP_DATASET_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY,
            )

        guid_to_package_id = self._existing_guid_to_package_id(harvest_job)
        object_ids = []
        guids_in_source = []
        total_layers = len(parsed["layers"])
        skipped_count = 0
        skipped_non_matching_title_count = 0
        skipped_missing_wfs_count = 0
        skipped_present_wfs_count = 0

        for index, layer in enumerate(parsed["layers"], start=1):
            title_matches_layer_name = self._title_matches_layer_name(layer)
            if include_only_title_matches_layer_name and not title_matches_layer_name:
                skipped_non_matching_title_count += 1
                if (
                    skipped_non_matching_title_count == 1
                    or skipped_non_matching_title_count % log_every == 0
                ):
                    log.info(
                        "Skipping WMS layer because title does not match layer "
                        "name: %d skipped so far; layer=%s title=%s",
                        skipped_non_matching_title_count,
                        layer["name"],
                        layer.get("title") or "",
                    )
                continue

            if (
                not include_only_title_matches_layer_name
                and skip_title_matches_layer_name
                and title_matches_layer_name
            ):
                skipped_count += 1
                if skipped_count == 1 or skipped_count % log_every == 0:
                    log.info(
                        "Skipping WMS layer with non-descriptive title: "
                        "%d skipped so far; layer=%s title=%s",
                        skipped_count,
                        layer["name"],
                        layer.get("title") or "",
                )
                continue

            layer_available_in_wfs = False
            if isinstance(wfs_info, dict):
                layer_available_in_wfs = bool(
                    self._wfs_feature_type_name_for_layer(layer["name"], wfs_info)
                )
            if include_only_layers_missing_from_wfs and layer_available_in_wfs:
                skipped_present_wfs_count += 1
                if (
                    skipped_present_wfs_count == 1
                    or skipped_present_wfs_count % log_every == 0
                ):
                    log.info(
                        "Skipping WMS layer present in WFS capabilities: "
                        "%d skipped so far; layer=%s",
                        skipped_present_wfs_count,
                        layer["name"],
                    )
                continue

            if (
                not include_only_layers_missing_from_wfs
                and skip_layers_missing_from_wfs
                and isinstance(wfs_info, dict)
                and not layer_available_in_wfs
            ):
                skipped_missing_wfs_count += 1
                if (
                    skipped_missing_wfs_count == 1
                    or skipped_missing_wfs_count % log_every == 0
                ):
                    log.info(
                        "Skipping WMS layer missing from WFS capabilities: "
                        "%d skipped so far; layer=%s",
                        skipped_missing_wfs_count,
                        layer["name"],
                    )
                continue

            dataset_name = self._dataset_name_from_layer_name(
                prefix,
                layer["name"],
                config,
            )
            if not dataset_name:
                self._save_gather_error(
                    "Skipping WMS layer with invalid CKAN name: %s" % layer["name"],
                    harvest_job,
                )
                continue

            guid = dataset_name
            guids_in_source.append(guid)

            if index == 1 or index == total_layers or index % log_every == 0:
                log.info(
                    "WMS capabilities gather progress: %d/%d layer=%s guid=%s",
                    index,
                    total_layers,
                    layer["name"],
                    guid,
                )

            payload = {
                "layer": layer,
                "dataset_name": dataset_name,
                "guid": guid,
                "service_title": parsed.get("service_title") or "",
                "source_url": harvest_job.source.url,
            }
            if isinstance(wfs_info, dict):
                payload["wfs"] = self._wfs_payload_for_layer(layer["name"], wfs_info)

            extras = [HarvestObjectExtra(key="status", value="change" if guid in guid_to_package_id else "new")]
            obj_kwargs = {
                "guid": guid,
                "job": harvest_job,
                "content": json.dumps(payload, ensure_ascii=False),
                "extras": extras,
            }
            if guid in guid_to_package_id:
                obj_kwargs["package_id"] = guid_to_package_id[guid]

            obj = HarvestObject(**obj_kwargs)
            obj.save()
            object_ids.append(obj.id)

        try:
            object_ids.extend(self._mark_datasets_for_deletion(guids_in_source, harvest_job))
        except Exception as e:
            log.warning("Error computing WMS deleted datasets: %s", e)

        log.info("WMS capabilities gather completed: %d objects", len(object_ids))
        if skipped_count:
            log.info(
                "WMS capabilities gather skipped %d layers with titles matching "
                "their layer names",
                skipped_count,
            )
        if skipped_non_matching_title_count:
            log.info(
                "WMS capabilities gather skipped %d layers with titles not "
                "matching their layer names",
                skipped_non_matching_title_count,
            )
        if skipped_missing_wfs_count:
            log.info(
                "WMS capabilities gather skipped %d layers missing from WFS "
                "capabilities",
                skipped_missing_wfs_count,
            )
        if skipped_present_wfs_count:
            log.info(
                "WMS capabilities gather skipped %d layers present in WFS "
                "capabilities",
                skipped_present_wfs_count,
            )
        return object_ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        status = self._get_object_extra(harvest_object, "status")
        if status == "delete":
            return self._delete_package_for_harvest_object(harvest_object)

        try:
            payload = json.loads(harvest_object.content)
            config = self._config_from_harvest_object(harvest_object)
            package_dict = self._package_dict_from_payload(payload, harvest_object, config)
            return self._create_or_update_package(package_dict, harvest_object)
        except Exception as e:
            log.exception("Error importing WMS layer")
            self._save_object_error(str(e), harvest_object, "Import")
            return False

    def _get_object_extra(self, harvest_object, key: str) -> str | None:
        for extra in getattr(harvest_object, "extras", []) or []:
            if getattr(extra, "key", None) == key:
                return getattr(extra, "value", None)
        return None

    def _delete_package_for_harvest_object(self, harvest_object):
        context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
            "ignore_auth": True,
        }
        try:
            toolkit.get_action("package_delete")(context, {"id": harvest_object.package_id})
            log.info(
                "Deleted package %s with WMS guid %s",
                harvest_object.package_id,
                harvest_object.guid,
            )
        except toolkit.ObjectNotFound:
            log.info("Package %s already deleted.", harvest_object.package_id)

        return True

    def _config(self, harvest_job) -> dict[str, Any]:
        if not harvest_job or not harvest_job.source or not harvest_job.source.config:
            return {}
        try:
            config = json.loads(harvest_job.source.config)
            return config if isinstance(config, dict) else {}
        except ValueError:
            return {}

    def _config_from_harvest_object(self, harvest_object) -> dict[str, Any]:
        source = getattr(getattr(harvest_object, "job", None), "source", None)
        if source is None:
            source = getattr(harvest_object, "source", None)
        if source is None or not getattr(source, "config", None):
            return {}
        try:
            config = json.loads(source.config)
            return config if isinstance(config, dict) else {}
        except ValueError:
            return {}

    def _dataset_name_prefix(self, config: dict[str, Any]) -> str:
        return str(
            config.get(DATASET_NAME_PREFIX_CONFIG_KEY)
            or config.get(LEGACY_DATASET_NAME_PREFIX_CONFIG_KEY)
            or ""
        )

    def _dataset_name_from_layer_name(
        self,
        prefix: str,
        layer_name: str,
        config: dict[str, Any],
    ) -> str:
        dataset_name = normalize_ckan_name("%s%s" % (prefix, layer_name))
        max_length = self._dataset_name_max_length(config)
        if not dataset_name or len(dataset_name) <= max_length:
            return dataset_name

        digest = sha1(dataset_name.encode("utf-8")).hexdigest()[:10]
        suffix = "-%s" % digest
        prefix_length = max_length - len(suffix)
        if prefix_length <= 0:
            return digest[:max_length]

        trimmed = dataset_name[:prefix_length].strip("-_")
        if not trimmed:
            return digest[:max_length]
        return "%s%s" % (trimmed, suffix)

    def _dataset_name_max_length(self, config: dict[str, Any]) -> int:
        try:
            max_length = int(config.get(
                DATASET_NAME_MAX_LENGTH_CONFIG_KEY,
                DEFAULT_DATASET_NAME_MAX_LENGTH,
            ))
        except (TypeError, ValueError):
            max_length = DEFAULT_DATASET_NAME_MAX_LENGTH
        return max(1, max_length)

    def _title_matches_layer_name(self, layer: dict[str, Any]) -> bool:
        layer_name = str(layer.get("name") or "").strip()
        title = str(layer.get("title") or "").strip()
        if not layer_name or not title:
            return False

        local_layer_name = layer_name.split(":", 1)[-1]
        normalized_title = self._normalize_title_for_match(title)
        return normalized_title in {
            self._normalize_title_for_match(layer_name),
            self._normalize_title_for_match(local_layer_name),
        }

    def _normalize_title_for_match(self, value: str) -> str:
        normalized = re.sub(r"\s+", " ", str(value or "").strip()).lower()
        return re.sub(r"[-_]+", "-", normalized)

    def _wfs_info_for_gather(
        self,
        config: dict[str, Any],
        harvest_job,
    ) -> dict[str, Any] | bool | None:
        if not self._requires_wfs_capabilities(config):
            return False

        capabilities_xml = self._load_wfs_capabilities(harvest_job, config)
        if not capabilities_xml:
            return None

        try:
            started_at = time.monotonic()
            wfs_info = parse_wfs_capabilities(capabilities_xml)
            log.info(
                "Parsed WFS capabilities: %d feature types in %.2fs",
                len(wfs_info.get("layer_names") or []),
                time.monotonic() - started_at,
            )
        except Exception as e:
            self._save_gather_error(
                "Could not parse WFS capabilities: %s" % e,
                harvest_job,
            )
            log.exception("Could not parse WFS capabilities")
            return None

        layer_names = wfs_info.get("layer_names") or set()
        if not layer_names:
            self._save_gather_error(
                "Could not find any WFS FeatureType names in capabilities document",
                harvest_job,
            )
            return None

        configured_getfeature_url = str(
            config.get(WFS_GETFEATURE_BASE_URL_CONFIG_KEY) or ""
        ).strip()
        if configured_getfeature_url:
            wfs_info["getfeature_base_url"] = configured_getfeature_url
        elif not wfs_info.get("getfeature_base_url"):
            wfs_info["getfeature_base_url"] = self._base_url_without_query(
                str(config.get(WFS_CAPABILITIES_URL_CONFIG_KEY) or "")
            )
        wfs_info["layer_name_prefix"] = str(
            config.get(WFS_LAYER_NAME_PREFIX_CONFIG_KEY) or ""
        ).strip()

        wfs_download_resource_configs = self._wfs_download_resource_configs(config)
        if (
            wfs_download_resource_configs
            and not wfs_info.get("getfeature_base_url")
            and any(
                not resource_config.get("base_url")
                for resource_config in wfs_download_resource_configs
            )
        ):
            self._save_gather_error(
                "No WFS GetFeature endpoint available for WFS download resources",
                harvest_job,
            )
            return None

        log.info(
            "Loaded %d WFS FeatureType names for WMS layer filtering",
            len(layer_names),
        )
        return wfs_info

    def _requires_wfs_capabilities(self, config: dict[str, Any]) -> bool:
        return bool(
            config.get(
                SKIP_DATASET_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY
            )
            or config.get(
                INCLUDE_ONLY_DATASETS_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY
            )
            or config.get(
                SKIP_WFS_CAPABILITIES_RESOURCE_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY
            )
            or config.get(
                SKIP_WMS_GETMAP_RESOURCES_WHEN_LAYER_PRESENT_IN_WFS_CAPABILITIES_CONFIG_KEY
            )
            or self._wfs_download_resource_configs(config)
        )

    def _wfs_payload_for_layer(
        self,
        layer_name: str,
        wfs_info: dict[str, Any],
    ) -> dict[str, Any]:
        feature_type_name = self._wfs_feature_type_name_for_layer(
            layer_name,
            wfs_info,
        )
        return {
            "layer_available": bool(feature_type_name),
            "feature_type_name": feature_type_name,
            "version": wfs_info.get("version") or "2.0.0",
            "getfeature_base_url": wfs_info.get("getfeature_base_url") or "",
            "output_formats": sorted(wfs_info.get("output_formats") or []),
        }

    def _wfs_feature_type_name_for_layer(
        self,
        layer_name: str,
        wfs_info: dict[str, Any],
    ) -> str:
        layer_name = str(layer_name or "").strip()
        if not layer_name:
            return ""

        layer_names = wfs_info.get("layer_names") or set()
        if layer_name in layer_names:
            return layer_name

        prefix = str(wfs_info.get("layer_name_prefix") or "").strip()
        if not prefix:
            return ""

        prefixed_layer_name = "%s%s" % (prefix, layer_name)
        if prefixed_layer_name in layer_names:
            return prefixed_layer_name

        return ""

    def _base_url_without_query(self, url: str) -> str:
        if not url:
            return ""
        parsed = urlsplit(url)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))

    def _load_capabilities(self, harvest_job, config: dict[str, Any]) -> bytes | None:
        capabilities_file = str(config.get("capabilities_file") or "").strip()
        if capabilities_file:
            try:
                with open(capabilities_file, "rb") as fh:
                    return fh.read()
            except Exception as e:
                self._save_gather_error(
                    "Could not read WMS capabilities file %s: %s" % (capabilities_file, e),
                    harvest_job,
                )
                return None

        url = str(config.get(WMS_CAPABILITIES_URL_CONFIG_KEY) or harvest_job.source.url or "").strip()
        if not url:
            self._save_gather_error("No WMS capabilities URL configured", harvest_job)
            return None

        try:
            verify_ssl = self._verify_ssl(config)
            if not verify_ssl:
                log.warning("SSL verification disabled for WMS capabilities: %s", url)
            log.info(
                "Fetching WMS capabilities: %s timeout=%s",
                url,
                self._request_timeout(config),
            )
            started_at = time.monotonic()
            response = requests.get(
                url,
                headers=self._request_headers(config),
                timeout=self._request_timeout(config),
                verify=verify_ssl,
            )
            response.raise_for_status()
            content = response.content
            log.info(
                "Fetched WMS capabilities: status=%s bytes=%d in %.2fs",
                response.status_code,
                len(content),
                time.monotonic() - started_at,
            )
            return content
        except Exception as e:
            self._save_gather_error("Could not fetch WMS capabilities %s: %s" % (url, e), harvest_job)
            return None

    def _load_wfs_capabilities(
        self,
        harvest_job,
        config: dict[str, Any],
    ) -> bytes | None:
        capabilities_file = str(
            config.get(WFS_CAPABILITIES_FILE_CONFIG_KEY) or ""
        ).strip()
        if capabilities_file:
            try:
                with open(capabilities_file, "rb") as fh:
                    return fh.read()
            except Exception as e:
                self._save_gather_error(
                    "Could not read WFS capabilities file %s: %s"
                    % (capabilities_file, e),
                    harvest_job,
                )
                return None

        url = str(config.get(WFS_CAPABILITIES_URL_CONFIG_KEY) or "").strip()
        if not url:
            self._save_gather_error(
                "No WFS capabilities URL configured for WFS layer filtering",
                harvest_job,
            )
            return None

        try:
            verify_ssl = self._verify_ssl(config)
            if not verify_ssl:
                log.warning("SSL verification disabled for WFS capabilities: %s", url)
            log.info(
                "Fetching WFS capabilities: %s timeout=%s",
                url,
                self._request_timeout(config),
            )
            started_at = time.monotonic()
            response = requests.get(
                url,
                headers=self._request_headers(config),
                timeout=self._request_timeout(config),
                verify=verify_ssl,
            )
            response.raise_for_status()
            content = response.content
            log.info(
                "Fetched WFS capabilities: status=%s bytes=%d in %.2fs",
                response.status_code,
                len(content),
                time.monotonic() - started_at,
            )
            return content
        except Exception as e:
            self._save_gather_error(
                "Could not fetch WFS capabilities %s: %s" % (url, e),
                harvest_job,
            )
            return None

    def _request_headers(self, config: dict[str, Any]) -> dict[str, str]:
        user_agent = str(config.get("user_agent") or "").strip()
        if not user_agent:
            return {}
        return {"User-Agent": user_agent}

    def _request_timeout(self, config: dict[str, Any]) -> int:
        try:
            timeout = int(config.get("timeout") or DEFAULT_TIMEOUT)
        except (TypeError, ValueError):
            timeout = DEFAULT_TIMEOUT
        return max(1, timeout)

    def _verify_ssl(self, config: dict[str, Any]) -> bool:
        return not self._bool_value(
            config.get(DISABLE_SSL_VERIFICATION_CONFIG_KEY),
            False,
        )

    def _gather_log_every(self, config: dict[str, Any]) -> int:
        try:
            log_every = int(config.get(GATHER_LOG_EVERY_CONFIG_KEY) or 100)
        except (TypeError, ValueError):
            log_every = 100
        return max(1, log_every)

    def _existing_guid_to_package_id(self, harvest_job) -> dict[str, str]:
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(HarvestObject.current == True)
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        )
        return {guid: package_id for guid, package_id in query}

    def _mark_datasets_for_deletion(self, guids_in_source, harvest_job):
        object_ids = []
        guid_to_package_id = self._existing_guid_to_package_id(harvest_job)
        guids_to_delete = set(guid_to_package_id.keys()) - set(guids_in_source)

        for guid in guids_to_delete:
            obj = HarvestObject(
                guid=guid,
                job=harvest_job,
                package_id=guid_to_package_id[guid],
                extras=[HarvestObjectExtra(key="status", value="delete")],
            )
            model.Session.query(HarvestObject).filter_by(guid=guid).update(
                {"current": False},
                False,
            )
            obj.save()
            object_ids.append(obj.id)

        return object_ids

    def _package_dict_from_payload(
        self,
        payload: dict[str, Any],
        harvest_object,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        layer = payload["layer"]
        layer_name = layer["name"]
        title = layer.get("title") or layer_name
        notes = layer.get("abstract") or title

        package_dict = {
            "name": payload["dataset_name"],
            "title": title,
            "title_translated": self._title_translated_for_layer(
                layer,
                title,
                config,
            ),
            "notes": notes,
            "notes_translated": _translated(notes, title),
            "private": bool(config.get("private", False)),
            "tag_string": self._tag_string(layer, config),
            "access_rights": PUBLIC_ACCESS_RIGHTS,
            "dcat_type": [GEOSPATIAL_DCAT_TYPE],
            "resources": self._resources(layer, payload, config),
            "extras": [
                {"key": "guid", "value": payload["guid"]},
                {"key": "wms_layer_name", "value": layer_name},
            ],
        }

        source_pkg = self._source_package(harvest_object)
        if source_pkg and source_pkg.owner_org:
            package_dict["owner_org"] = source_pkg.owner_org

        if layer.get("bbox"):
            package_dict["spatial_coverage"] = [{
                "uri": "",
                "text": "",
                "geom": "",
                "bbox": _bbox_geojson(layer["bbox"]),
                "centroid": _centroid_geojson(layer["bbox"]),
            }]

        themes = config.get(DEFAULT_THEME_CONFIG_KEY)
        if isinstance(themes, list) and themes:
            package_dict["theme"] = themes

        apply_default_dataset_fields_from_config(package_dict, harvest_object)
        ensure_applicable_legislation(package_dict, protected=False)
        apply_default_resource_fields_from_config(package_dict, harvest_object)
        preserve_resource_ids_by_url(package_dict, harvest_object)
        return package_dict

    def _title_translated_for_layer(
        self,
        layer: dict[str, Any],
        title: str,
        config: dict[str, Any],
    ) -> dict[str, str]:
        layer_name = str(layer.get("name") or "").strip()
        prefix = str(
            config.get(TITLE_PREFIX_FOR_LAYER_NAME_TITLES_CONFIG_KEY) or ""
        )
        if not prefix or not self._title_matches_layer_name(layer):
            return _translated(title, layer_name)

        return _translated("%s%s" % (prefix, title), layer_name)

    def _source_package(self, harvest_object):
        source = getattr(getattr(harvest_object, "job", None), "source", None)
        if source is None:
            source = getattr(harvest_object, "source", None)
        source_id = getattr(source, "id", None)
        return model.Package.get(source_id) if source_id else None

    def _tag_string(self, layer: dict[str, Any], config: dict[str, Any]) -> str:
        tags = []
        layer_name = str(layer.get("name") or "").strip()
        local_layer_name = layer_name.split(":", 1)[-1] if layer_name else ""
        include_layer_name_keywords = bool(
            config.get(INCLUDE_LAYER_NAME_KEYWORDS_CONFIG_KEY)
        )
        skip_keyword_patterns = self._compiled_skip_keyword_patterns(config)
        skipped_values = {
            layer_name.lower(),
            local_layer_name.lower(),
        }
        for value in layer.get("keywords") or []:
            value = str(value or "").strip()
            if not value:
                continue
            if not include_layer_name_keywords and value.lower() in skipped_values:
                continue
            tag = normalize_ckan_tag(value)
            if self._keyword_matches_any_pattern(value, skip_keyword_patterns):
                continue
            if self._keyword_matches_any_pattern(tag, skip_keyword_patterns):
                continue
            if len(tag) < 2:
                continue
            if tag and tag not in tags:
                tags.append(tag)
        for tag in self._default_tags(config):
            if tag not in tags:
                tags.append(tag)
        return ", ".join(tags)

    def _default_tags(self, config: dict[str, Any]) -> list[str]:
        raw_tags = config.get(DEFAULT_TAGS_CONFIG_KEY)
        if isinstance(raw_tags, str):
            raw_tags = [raw_tags]
        if not isinstance(raw_tags, list):
            return []

        tags = []
        for value in raw_tags:
            value = str(value or "").strip()
            if not value:
                continue
            tag = normalize_ckan_tag(value)
            if len(tag) < 2:
                continue
            if tag and tag not in tags:
                tags.append(tag)
        return tags

    def _compiled_skip_keyword_patterns(self, config: dict[str, Any]) -> list[Any]:
        raw_patterns = config.get(SKIP_KEYWORDS_MATCHING_CONFIG_KEY)
        if isinstance(raw_patterns, str):
            raw_patterns = [raw_patterns]
        if not isinstance(raw_patterns, list):
            return []

        patterns = []
        for raw_pattern in raw_patterns:
            if not isinstance(raw_pattern, str) or not raw_pattern.strip():
                continue
            try:
                patterns.append(re.compile(raw_pattern.strip(), re.IGNORECASE))
            except re.error as e:
                log.warning(
                    "Ignoring invalid WMS keyword skip regex %r: %s",
                    raw_pattern,
                    e,
                )
        return patterns

    def _keyword_matches_any_pattern(self, keyword: str, patterns: list[Any]) -> bool:
        return any(pattern.search(keyword) for pattern in patterns)

    def _resources(
        self,
        layer: dict[str, Any],
        payload: dict[str, Any],
        config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        layer_name = layer["name"]
        source_url = payload.get("source_url") or ""
        wms_capabilities_url = str(
            config.get(WMS_CAPABILITIES_URL_CONFIG_KEY) or source_url
        ).strip()
        wfs_capabilities_url = str(config.get(WFS_CAPABILITIES_URL_CONFIG_KEY) or "").strip()
        wms_preview_base_url = str(config.get(WMS_PREVIEW_BASE_URL_CONFIG_KEY) or "").strip()

        resources = []
        if wms_preview_base_url:
            preview_resource = self._resource(
                url=self._wms_preview_url(layer_name, config),
                layer_name=layer_name,
                title_el="Προεπισκόπηση WMS layer - %s" % layer_name,
                title_en="WMS layer preview - %s" % layer_name,
                description_el="Προεπισκόπηση του WMS layer %s." % layer_name,
                description_en="Preview of WMS layer %s." % layer_name,
                resource_format="WMS",
                protocol="OGC:WMS",
            )
            if self._bool_value(
                config.get(WMS_PREVIEW_RESOURCE_URLS_USE_DATASET_URL_CONFIG_KEY),
                False,
            ):
                dataset_url = self._dataset_url(payload)
                if dataset_url:
                    preview_resource["access_url"] = dataset_url
                    preview_resource["download_url"] = dataset_url
            resources.append(preview_resource)

        resources.extend(self._wms_getmap_resources(layer, payload, config))
        resources.extend(self._wfs_download_resources(layer, payload, config))

        if wms_capabilities_url:
            resource = self._resource(
                url=wms_capabilities_url,
                layer_name=layer_name,
                title_el="WMS capabilities document - %s" % layer_name,
                title_en="WMS capabilities document - %s" % layer_name,
                description_el=(
                    "WMS GetCapabilities document που περιγράφει και το layer %s."
                    % layer_name
                ),
                description_en=(
                    "WMS GetCapabilities document that includes layer %s."
                    % layer_name
                ),
                resource_format="XML",
                protocol="OGC:WMS",
            )
            resource["download_url"] = wms_capabilities_url
            resource["mimetype"] = XML_MIMETYPE
            resources.append(resource)

        if (
            wfs_capabilities_url
            and not self._skip_wfs_capabilities_resource(payload, config)
        ):
            resource = self._resource(
                url=wfs_capabilities_url,
                layer_name=layer_name,
                title_el="WFS capabilities document - %s" % layer_name,
                title_en="WFS capabilities document - %s" % layer_name,
                description_el=(
                    "WFS GetCapabilities document που μπορεί να χρησιμοποιηθεί για "
                    "αναζήτηση του layer %s."
                    % layer_name
                ),
                description_en=(
                    "WFS GetCapabilities document that can be used to look up layer %s."
                    % layer_name
                ),
                resource_format="XML",
                protocol="OGC:WFS",
            )
            resource["download_url"] = wfs_capabilities_url
            resource["mimetype"] = XML_MIMETYPE
            resources.append(resource)

        return resources

    def _skip_wfs_capabilities_resource(
        self,
        payload: dict[str, Any],
        config: dict[str, Any],
    ) -> bool:
        if not config.get(
            SKIP_WFS_CAPABILITIES_RESOURCE_WHEN_LAYER_MISSING_FROM_WFS_CAPABILITIES_CONFIG_KEY
        ):
            return False

        wfs_payload = payload.get("wfs") or {}
        return not wfs_payload.get("layer_available")

    def _wms_getmap_resources(
        self,
        layer: dict[str, Any],
        payload: dict[str, Any],
        config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if self._skip_wms_getmap_resources(payload, config):
            return []

        bbox = layer.get("bbox")
        if not isinstance(bbox, dict):
            return []

        source_url = str(payload.get("source_url") or "").strip()
        base_url = str(
            config.get(WMS_GETMAP_BASE_URL_CONFIG_KEY)
            or config.get(WMS_CAPABILITIES_URL_CONFIG_KEY)
            or source_url
        ).strip()
        if not self._bool_value(
            config.get(WMS_GETMAP_BASE_URL_PRESERVE_QUERY_CONFIG_KEY),
            False,
        ):
            base_url = self._base_url_without_query(base_url)
        if not base_url:
            return []

        resources = []
        crs_values = set(layer.get("crs") or [])
        for resource_config in self._wms_getmap_resource_configs(config):
            crs = resource_config["crs"]
            if crs not in crs_values:
                log.warning(
                    "Skipping WMS GetMap resource for unsupported CRS %s; "
                    "layer=%s",
                    crs,
                    layer["name"],
                )
                continue

            url = self._wms_getmap_url(
                base_url=base_url,
                layer_name=layer["name"],
                bbox=bbox,
                version=resource_config["version"],
                crs=crs,
                image_format=resource_config["image_format"],
                width=resource_config["width"],
                height=resource_config["height"],
                transparent=resource_config["transparent"],
                extra_params=resource_config.get("params") or {},
            )
            resource_format = resource_config["format"]
            title_el = "Λήψη εικόνας %s - %s" % (resource_format, layer["name"])
            title_en = "%s image download - %s" % (resource_format, layer["name"])
            resource = {
                "url": url,
                "name_translated": {
                    "el": title_el,
                    "en": title_en,
                },
                "description_translated": {
                    "el": title_el,
                    "en": title_en,
                },
                "format": resource_format,
                "resource_locator_protocol": "OGC:WMS",
                "access_url": url,
                "download_url": url,
            }
            mimetype = resource_config.get("mimetype")
            if mimetype:
                resource["mimetype"] = mimetype
            resources.append(resource)

        return resources

    def _skip_wms_getmap_resources(
        self,
        payload: dict[str, Any],
        config: dict[str, Any],
    ) -> bool:
        if not config.get(
            SKIP_WMS_GETMAP_RESOURCES_WHEN_LAYER_PRESENT_IN_WFS_CAPABILITIES_CONFIG_KEY
        ):
            return False

        wfs_payload = payload.get("wfs") or {}
        return bool(wfs_payload.get("layer_available"))

    def _wms_getmap_resource_configs(
        self,
        config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        raw_resources = config.get(WMS_GETMAP_RESOURCES_CONFIG_KEY)
        if not isinstance(raw_resources, list):
            return []

        resources = []
        for raw_resource in raw_resources:
            if not isinstance(raw_resource, dict):
                continue

            image_format = str(
                raw_resource.get("image_format") or "image/png"
            ).strip()
            if not image_format:
                continue

            resource_format = str(raw_resource.get("format") or "PNG").strip()
            if not resource_format:
                resource_format = "PNG"

            mimetype = str(raw_resource.get("mimetype") or "").strip()
            if not mimetype and image_format == "image/png":
                mimetype = PNG_MIMETYPE

            params = raw_resource.get("params") or {}
            if not isinstance(params, dict):
                params = {}

            resources.append({
                "format": resource_format,
                "image_format": image_format,
                "mimetype": mimetype,
                "width": self._positive_int(raw_resource.get("width"), 2048),
                "height": self._positive_int(raw_resource.get("height"), 2048),
                "crs": str(raw_resource.get("crs") or "CRS:84").strip() or "CRS:84",
                "version": str(raw_resource.get("version") or "1.3.0").strip()
                or "1.3.0",
                "transparent": self._bool_value(
                    raw_resource.get("transparent", True),
                    True,
                ),
                "params": params,
            })

        return resources

    def _bool_value(self, value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False
        if value is None:
            return default
        return bool(value)

    def _positive_int(self, value: Any, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(1, parsed)

    def _wms_getmap_url(
        self,
        *,
        base_url: str,
        layer_name: str,
        bbox: dict[str, float],
        version: str,
        crs: str,
        image_format: str,
        width: int,
        height: int,
        transparent: bool,
        extra_params: dict[str, Any],
    ) -> str:
        crs_param = "crs" if str(version).startswith("1.3") else "srs"
        params = [
            ("service", "WMS"),
            ("version", version),
            ("request", "GetMap"),
            ("layers", layer_name),
            ("styles", ""),
            (crs_param, crs),
            ("bbox", self._wms_getmap_bbox_value(bbox, crs, version)),
            ("width", width),
            ("height", height),
            ("format", image_format),
            ("transparent", str(transparent).lower()),
        ]
        reserved_keys = {
            "service",
            "version",
            "request",
            "layers",
            "styles",
            "crs",
            "srs",
            "bbox",
            "width",
            "height",
            "format",
            "transparent",
        }
        for key in sorted(extra_params):
            if key in reserved_keys:
                continue
            value = extra_params[key]
            if value is None:
                continue
            params.append((key, value))

        separator = "&" if "?" in base_url else "?"
        return "%s%s%s" % (
            base_url,
            separator,
            urlencode(params, doseq=True),
        )

    def _wms_getmap_bbox_value(
        self,
        bbox: dict[str, float],
        crs: str,
        version: str,
    ) -> str:
        if str(version).startswith("1.3") and crs.upper() == "EPSG:4326":
            values = [bbox["south"], bbox["west"], bbox["north"], bbox["east"]]
        else:
            values = [bbox["west"], bbox["south"], bbox["east"], bbox["north"]]
        return ",".join(str(value) for value in values)

    def _wfs_download_resources(
        self,
        layer: dict[str, Any],
        payload: dict[str, Any],
        config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        wfs_payload = payload.get("wfs") or {}
        if not wfs_payload.get("layer_available"):
            return []

        getfeature_base_url = str(
            wfs_payload.get("getfeature_base_url") or ""
        ).strip()

        output_formats = set(wfs_payload.get("output_formats") or [])
        resources = []
        for resource_config in self._wfs_download_resource_configs(config):
            output_format = resource_config["output_format"]
            if output_format not in output_formats:
                log.warning(
                    "Skipping WFS download resource for unsupported outputFormat "
                    "%s; layer=%s",
                    output_format,
                    layer["name"],
                )
                continue

            resource_format = resource_config["format"]
            resource_base_url = (
                resource_config.get("base_url") or getfeature_base_url
            )
            if not resource_base_url:
                log.warning(
                    "Skipping WFS download resource with no GetFeature base URL; "
                    "format=%s layer=%s",
                    resource_format,
                    layer["name"],
                )
                continue

            url = self._wfs_getfeature_url(
                base_url=resource_base_url,
                layer_name=wfs_payload.get("feature_type_name") or layer["name"],
                version=str(wfs_payload.get("version") or "2.0.0"),
                output_format=output_format,
                extra_params=resource_config.get("params") or {},
            )
            title_el = "Λήψη %s - %s" % (resource_format, layer["name"])
            title_en = "%s download - %s" % (resource_format, layer["name"])
            resources.append({
                "url": url,
                "name_translated": {
                    "el": title_el,
                    "en": title_en,
                },
                "description_translated": {
                    "el": title_el,
                    "en": title_en,
                },
                "format": resource_format,
                "resource_locator_protocol": "OGC:WFS",
                "access_url": url,
                "download_url": url,
            })
            mimetype = resource_config.get("mimetype")
            if mimetype:
                resources[-1]["mimetype"] = mimetype

        return resources

    def _wfs_download_resource_configs(
        self,
        config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        raw_resources = config.get(WFS_DOWNLOAD_RESOURCES_CONFIG_KEY)
        if not isinstance(raw_resources, list):
            return []

        resources = []
        for raw_resource in raw_resources:
            if not isinstance(raw_resource, dict):
                continue

            output_format = str(raw_resource.get("output_format") or "").strip()
            if not output_format:
                continue

            resource_format = str(
                raw_resource.get("format") or output_format
            ).strip()
            if not resource_format:
                continue

            params = raw_resource.get("params") or {}
            if not isinstance(params, dict):
                params = {}

            resources.append({
                "format": resource_format,
                "output_format": output_format,
                "base_url": str(raw_resource.get("base_url") or "").strip(),
                "params": params,
                "mimetype": str(raw_resource.get("mimetype") or "").strip(),
            })

        return resources

    def _wfs_getfeature_url(
        self,
        *,
        base_url: str,
        layer_name: str,
        version: str,
        output_format: str,
        extra_params: dict[str, Any],
    ) -> str:
        type_name_param = (
            "typeNames" if str(version).startswith("2.") else "typeName"
        )
        params = [
            ("service", "WFS"),
            ("version", version),
            ("request", "GetFeature"),
            (type_name_param, layer_name),
            ("outputFormat", output_format),
        ]
        reserved_keys = {key for key, _value in params}
        for key in sorted(extra_params):
            if key in reserved_keys:
                continue
            value = extra_params[key]
            if value is None:
                continue
            params.append((key, value))

        separator = "&" if "?" in base_url else "?"
        return "%s%s%s" % (
            base_url,
            separator,
            urlencode(params, doseq=True),
        )

    def _wms_preview_url(self, layer_name: str, config: dict[str, Any]) -> str:
        base_url = str(config.get(WMS_PREVIEW_BASE_URL_CONFIG_KEY) or "").strip()
        if not base_url:
            return ""

        if not config.get(WMS_PREVIEW_WORKSPACE_IN_PATH_CONFIG_KEY):
            return "%s%s" % (base_url, layer_name)

        workspace, separator, local_layer_name = str(layer_name or "").partition(":")
        if not separator or not workspace or not local_layer_name:
            return "%s%s" % (base_url, layer_name)

        parsed = urlsplit(base_url)
        path = parsed.path.rstrip("/")
        if not path.lower().endswith("/wms"):
            return "%s%s" % (base_url, layer_name)

        preview_path = "%s/%s/wms" % (path[:-4], workspace)
        return urlunsplit((
            parsed.scheme,
            parsed.netloc,
            preview_path,
            parsed.query,
            local_layer_name,
        ))

    def _dataset_url(self, payload: dict[str, Any]) -> str:
        site_url = str(toolkit.config.get("ckan.site_url") or "").strip().rstrip("/")
        dataset_name = str(payload.get("dataset_name") or "").strip()
        if not site_url or not dataset_name:
            return ""
        return "%s/dataset/%s" % (site_url, dataset_name)

    def _resource(
        self,
        *,
        url: str,
        layer_name: str,
        title_el: str,
        title_en: str,
        description_el: str,
        description_en: str,
        resource_format: str,
        protocol: str,
    ) -> dict[str, Any]:
        return {
            "url": url,
            "name": layer_name,
            "name_translated": {
                "el": title_el,
                "en": title_en,
            },
            "description_translated": {
                "el": description_el,
                "en": description_en,
            },
            "format": resource_format,
            "resource_locator_protocol": protocol,
            "access_url": url,
        }

    def _create_or_update_package(self, package_dict, harvest_object):
        user_name = self._get_user_name()
        context = {
            "model": model,
            "session": Session,
            "user": user_name,
            "ignore_auth": True,
        }

        try:
            if getattr(harvest_object, "package_id", None):
                package_dict["id"] = harvest_object.package_id
            existing_package_dict = self._find_existing_package(package_dict)
            package_dict["id"] = existing_package_dict["id"]
            package_dict["name"] = existing_package_dict["name"]
            new_package = toolkit.get_action("package_update")(context, package_dict)
        except (toolkit.ObjectNotFound, KeyError):
            new_package = toolkit.get_action("package_create")(context, package_dict)
        except toolkit.ValidationError as e:
            log.exception("Invalid WMS package with GUID %s", harvest_object.guid)
            self._save_object_error(
                "Invalid package with GUID %s: %s" % (harvest_object.guid, e.error_dict),
                harvest_object,
                "Import",
            )
            return False

        Session.query(HarvestObject).filter(
            HarvestObject.package_id == new_package["id"],
        ).update({"current": False})

        harvest_object.package_id = new_package["id"]
        harvest_object.current = True
        harvest_object.save()
        Session.commit()
        return True
