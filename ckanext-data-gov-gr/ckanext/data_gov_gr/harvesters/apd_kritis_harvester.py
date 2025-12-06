# -*- coding: utf-8 -*-
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .custom_dcat_harvester import CustomDcatHarvester

log = logging.getLogger(__name__)

# Simple in-memory cache for POD catalog JSON to avoid re-downloading
_pod_catalog_cache: Dict[str, Dict[str, Any]] = {}

# Static mapping from APD Kritis POD `theme` labels to data.gov.gr
# High-value dataset category URIs (as configured in the
# "High-value dataset categories" vocabulary).
_APD_THEME_TO_HVD_CATEGORY: Dict[str, str] = {
    "Περιβάλλον": "http://data.europa.eu/bna/c_dd313021",
    "Ύδατα": "http://data.europa.eu/bna/c_dd313021",
    "Ενέργεια και Φυσικοί Πόροι": "http://data.europa.eu/bna/c_dd313021",
    "Χαρτογραφικά Δεδομένα Ψηφιακών Χαρτών": "http://data.europa.eu/bna/c_ac64a52d",
    "Χωροταξικός Σχεδιασμός": "http://data.europa.eu/bna/c_ac64a52d",
    "Μετεωρολογικοί - Υδρολογικοί Σταθμοί": "http://data.europa.eu/bna/c_164e0bf5",
    "Ιστορικά δεδομένα προγραμμάτων": "http://data.europa.eu/bna/c_e1da4e07",
}

# Static mapping from APD Kritis POD `theme` labels to EU Data Theme
# authority URIs (as configured in the "Data theme" vocabulary).
#
# Vocabulary (from CKAN):
#   AGRI -> .../data-theme/AGRI
#   ECON -> .../data-theme/ECON
#   EDUC -> .../data-theme/EDUC
#   ENER -> .../data-theme/ENER
#   ENVI -> .../data-theme/ENVI
#   GOVE -> .../data-theme/GOVE
#   HEAL -> .../data-theme/HEAL
#   INTR -> .../data-theme/INTR
#   JUST -> .../data-theme/JUST
#   REGI -> .../data-theme/REGI
#   SOCI -> .../data-theme/SOCI
#   TECH -> .../data-theme/TECH
#   TRAN -> .../data-theme/TRAN
#
# Mapping decisions for APD Kritis themes:
#   - Περιβάλλον, Ύδατα, Μετεωρολογικοί - Υδρολογικοί Σταθμοί
#       -> ENVI
#   - Ενέργεια και Φυσικοί Πόροι
#       -> ENER
#   - Χαρτογραφικά Δεδομένα Ψηφιακών Χαρτών, Χωροταξικός Σχεδιασμός
#       -> REGI
#   - Ιστορικά δεδομένα προγραμμάτων
#       -> SOCI
_APD_THEME_TO_DATA_THEME: Dict[str, str] = {
    "Περιβάλλον": "http://publications.europa.eu/resource/authority/data-theme/ENVI",
    "Ύδατα": "http://publications.europa.eu/resource/authority/data-theme/ENVI",
    "Μετεωρολογικοί - Υδρολογικοί Σταθμοί": "http://publications.europa.eu/resource/authority/data-theme/ENVI",
    "Ενέργεια και Φυσικοί Πόροι": "http://publications.europa.eu/resource/authority/data-theme/ENER",
    "Χαρτογραφικά Δεδομένα Ψηφιακών Χαρτών": "http://publications.europa.eu/resource/authority/data-theme/REGI",
    "Χωροταξικός Σχεδιασμός": "http://publications.europa.eu/resource/authority/data-theme/REGI",
    "Ιστορικά δεδομένα προγραμμάτων": "http://publications.europa.eu/resource/authority/data-theme/SOCI",
}


class ApdKritisHarvester(CustomDcatHarvester):
    """
    Custom DCAT/POD harvester for APD Kritis (data.apdkritis.gov.gr).

    Extends CustomDcatHarvester and adds:

      - Recovery of dataset-level CC-BY licence from the POD data.json
        (where it is exposed as ``license: "cc-by"``) and injection into
        the harvested dataset dict so that the base class can normalise
        it to the EU authority URI.

      - Propagation of the normalised dataset licence down to all
        resources (handled by CustomDcatHarvester via
        ``_preserve_resource_level_licenses``), ensuring that resource
        licences use the expected EU authority URI.

      - Mapping of High Value Datasets (keyword
        "High Value Dataset" on the source) and their POD theme into
        data.gov.gr's ``hvd_category`` field using a static in-code
        mapping.

      - Mapping of POD ``theme`` labels into the EU Data Theme
        authority URIs, populating the dataset ``theme`` field with
        the correct URIs from the \"Data theme\" vocabulary.

    Harvest source configuration (JSON in the CKAN UI):

      - ``rdf_format`` (string, optional)
            RDF format hint for the remote catalog. For APD Kritis POD
            ``data.json`` this should be ``\"json-ld\"``. If omitted,
            it defaults to ``\"json-ld\"``.

      - Δεν απαιτείται άλλο configuration: οι αντιστοιχίσεις HVD και
        Data Theme είναι ενσωματωμένες στον κώδικα για τα themes του
        APD Κρήτης.
    """

    def info(self):
        return {
            "name": "apd_kritis_harvester",
            "title": "APD Kritis DCAT/POD Harvester",
            "description": (
                "Harvester for APD Kritis POD data.json that propagates "
                "CC-BY licences to resources and maps High Value datasets "
                "to hvd_category."
            ),
            "form_config_interface": "Text",
            "show_config": True,
        }

    def validate_config(self, source_config: Optional[str]):
        """
        Validate and normalise harvest source configuration.

        Ensures a sensible default for rdf_format (json-ld) for POD
        data.json sources.
        """
        if not source_config:
            # Default to JSON-LD for POD data.json
            return json.dumps({"rdf_format": "json-ld"})

        try:
            conf = json.loads(source_config) or {}
        except ValueError:
            # Delegate to parent for consistent error handling
            return super().validate_config(source_config)

        # Ensure rdf_format is set (POD data.json is JSON-LD)
        if not conf.get("rdf_format"):
            conf["rdf_format"] = "json-ld"

        return super().validate_config(json.dumps(conf))

    def modify_package_dict(self, package_dict, temp_dict, harvest_object):
        """
        Apply APD Kritis-specific mapping on top of the generic custom DCAT
        mapping from CustomDcatHarvester.

        Pre-parent:
          - Load the corresponding POD dataset from data.json
          - Inject dataset-level licence when available (eg. \"cc-by\")

        Post-parent:
          - Derive hvd_category from POD theme + High Value keyword
            using the static in-code mapping.
        """
        source_dataset = None
        try:
            source_dataset = self._get_pod_source_dataset(harvest_object)
        except Exception as e:
            log.error(
                "[APD KRITIS HARVESTER] Error loading POD source dataset: %s", e, exc_info=True
            )

        # Pre-parent: inject dataset-level licence so the base class can
        # normalise it and propagate it to resources.
        try:
            if source_dataset:
                self._inject_cc_by_license_from_source(package_dict, source_dataset)
        except Exception as e:
            log.error(
                "[APD KRITIS HARVESTER] Error applying pre-parent mapping: %s",
                e,
                exc_info=True,
            )

        # Run parent logic (all generic validation fixes / mappings)
        package_dict = super().modify_package_dict(package_dict, temp_dict, harvest_object)

        # Post-parent: derive HVD category and Data Theme from the POD
        # metadata when available.
        try:
            if source_dataset:
                self._apply_hvd_category_from_source(package_dict, source_dataset, harvest_object)
                self._apply_data_theme_from_source(package_dict, source_dataset, harvest_object)
        except Exception as e:
            log.error(
                "[APD KRITIS HARVESTER] Error applying post-parent mapping: %s",
                e,
                exc_info=True,
            )

        return package_dict

    # ------------------------------------------------------------------
    # POD /data.json helpers
    # ------------------------------------------------------------------

    def _get_pod_source_dataset(self, harvest_object) -> Optional[Dict[str, Any]]:
        """
        Load the original POD dataset entry from data.json that corresponds
        to the given harvest object, based on the HarvestObject.guid which
        is derived from dcat:identifier.
        """
        if not harvest_object:
            return None

        guid = getattr(harvest_object, "guid", None)
        job = getattr(harvest_object, "job", None)
        source = getattr(job, "source", None) if job else None
        source_url = getattr(source, "url", None) if source else None

        if not guid or not source_url:
            return None

        catalog, catalog_url = self._get_pod_catalog(source_url, job)
        if not catalog:
            return None

        datasets = catalog.get("dataset") or []
        for candidate in datasets:
            if not isinstance(candidate, dict):
                continue

            ident = candidate.get("identifier") or candidate.get("id") or candidate.get("@id")
            identifiers: List[str] = []

            if isinstance(ident, list):
                identifiers = [str(v) for v in ident]
            elif ident is not None:
                identifiers = [str(ident)]

            for value in identifiers:
                if value == guid or (guid and str(value).endswith(guid)):
                    log.debug(
                        "[APD KRITIS HARVESTER] Matched POD dataset %s in %s", guid, catalog_url
                    )
                    return candidate

        log.warning(
            "[APD KRITIS HARVESTER] No matching POD dataset for guid=%s in %s", guid, catalog_url
        )
        return None

    def _get_pod_catalog(
        self, source_url: str, harvest_job
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Resolve and fetch the POD data.json catalog for the given harvest
        source URL, with a simple in-process cache.
        """
        if not source_url:
            return None, None

        url = source_url.strip()
        if not url:
            return None, None

        base = url.rstrip("/")
        if base.endswith("data.json"):
            catalog_url = base
        else:
            # Fallback for sources pointed at the site root, eg
            # https://data.apdkritis.gov.gr
            catalog_url = base + "/sites/default/files/pod_data/data.json"

        if catalog_url in _pod_catalog_cache:
            return _pod_catalog_cache[catalog_url], catalog_url

        try:
            # Reuse the DCATRDFHarvester HTTP helper to honour proxies, etc.
            content, _ = self._get_content_and_type(
                catalog_url, harvest_job, page=1, content_type="application/json"
            )
        except Exception as e:
            log.error(
                "[APD KRITIS HARVESTER] Error fetching POD catalog %s: %s",
                catalog_url,
                e,
                exc_info=True,
            )
            return None, catalog_url

        if not content:
            log.warning(
                "[APD KRITIS HARVESTER] Empty content returned for POD catalog %s", catalog_url
            )
            return None, catalog_url

        try:
            data = json.loads(content)
        except ValueError as e:
            log.error(
                "[APD KRITIS HARVESTER] Error parsing POD catalog JSON from %s: %s",
                catalog_url,
                e,
                exc_info=True,
            )
            return None, catalog_url

        if not isinstance(data, dict):
            log.warning(
                "[APD KRITIS HARVESTER] POD catalog %s is not a JSON object, got %r",
                catalog_url,
                type(data),
            )
            return None, catalog_url

        _pod_catalog_cache[catalog_url] = data
        return data, catalog_url

    # ------------------------------------------------------------------
    # Licence handling
    # ------------------------------------------------------------------

    def _inject_cc_by_license_from_source(
        self, package_dict: Dict[str, Any], source_dataset: Dict[str, Any]
    ) -> None:
        """
        If the POD source dataset declares a CC-BY licence at dataset
        level (``license: \"cc-by\"``), inject it into the harvested
        package dict so that the base class can normalise it to the EU
        authority URI and propagate it to all resources.
        """
        if not isinstance(package_dict, dict) or not isinstance(source_dataset, dict):
            return

        license_value = source_dataset.get("license") or source_dataset.get("license_id")
        if not license_value or not isinstance(license_value, str):
            return

        value = license_value.strip().lower()
        if not value:
            return

        # APD Kritis uses "cc-by" for all datasets; be slightly tolerant
        # with variants.
        if value not in ("cc-by", "cc by", "cc_by", "creative commons attribution", "cc-by-4.0"):
            return

        # Only inject if there is no existing licence on the dataset
        if not package_dict.get("license") and not package_dict.get("license_id"):
            package_dict["license"] = "cc-by"
            package_dict["license_id"] = "cc-by"
            log.debug(
                "[APD KRITIS HARVESTER] Injected dataset-level licence 'cc-by' "
                "for downstream normalisation"
            )

    # ------------------------------------------------------------------
    # High Value Dataset (HVD) handling
    # ------------------------------------------------------------------

    def _apply_hvd_category_from_source(
        self,
        package_dict: Dict[str, Any],
        source_dataset: Dict[str, Any],
        harvest_object,
    ) -> None:
        """
        When the POD dataset is marked as High Value (keyword
        \"High Value Dataset\"), derive ``hvd_category`` using the
        static in-code theme -> HVD category URI mapping.
        """
        if not isinstance(package_dict, dict) or not isinstance(source_dataset, dict):
            return

        # Detect High Value datasets based on POD keywords
        keywords = source_dataset.get("keyword") or []
        keyword_values: List[str] = []
        if isinstance(keywords, list):
            keyword_values = [str(k) for k in keywords if k is not None]
        elif isinstance(keywords, str):
            keyword_values = [keywords]

        is_hvd = any(
            k.strip().lower() == "high value dataset"
            for k in keyword_values
            if isinstance(k, str) and k.strip()
        )

        if not is_hvd:
            return

        themes = source_dataset.get("theme") or []
        theme_values: List[str] = []
        if isinstance(themes, list):
            theme_values = [str(t) for t in themes if t is not None]
        elif isinstance(themes, str):
            theme_values = [themes]

        uris: List[str] = []
        seen: set = set()

        # Map themes directly to configured HVD category URIs
        for theme in theme_values:
            key = theme.strip()
            if not key:
                continue
            uri = _APD_THEME_TO_HVD_CATEGORY.get(key)
            if uri and isinstance(uri, str):
                if uri not in seen:
                    uris.append(uri)
                    seen.add(uri)

        if not uris:
            log.info(
                "[APD KRITIS HARVESTER] Dataset marked as High Value but no "
                "HVD category URI could be derived from theme/default mapping"
            )
            return

        package_dict["hvd_category"] = uris
        log.debug(
            "[APD KRITIS HARVESTER] Applied hvd_category=%r based on POD theme/keyword",
            uris,
        )

    # ------------------------------------------------------------------
    # Data Theme (EU authority) handling
    # ------------------------------------------------------------------

    def _apply_data_theme_from_source(
        self,
        package_dict: Dict[str, Any],
        source_dataset: Dict[str, Any],
        harvest_object,
    ) -> None:
        """
        Map POD ``theme`` labels from APD Kritis into EU Data Theme
        authority URIs and populate the dataset ``theme`` field.
        """
        if not isinstance(package_dict, dict) or not isinstance(source_dataset, dict):
            return

        themes = source_dataset.get("theme") or []
        theme_values: List[str] = []
        if isinstance(themes, list):
            theme_values = [str(t) for t in themes if t is not None]
        elif isinstance(themes, str):
            theme_values = [themes]

        uris: List[str] = []
        seen: set = set()

        for theme in theme_values:
            key = theme.strip()
            if not key:
                continue
            uri = _APD_THEME_TO_DATA_THEME.get(key)
            if uri and isinstance(uri, str) and uri not in seen:
                uris.append(uri)
                seen.add(uri)

        if not uris:
            return

        package_dict["theme"] = uris
        log.debug(
            "[APD KRITIS HARVESTER] Applied data theme(s)=%r based on POD theme labels",
            uris,
        )
