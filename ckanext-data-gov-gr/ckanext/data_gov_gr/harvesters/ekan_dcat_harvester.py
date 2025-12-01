import html
import json
import logging
import re
import time
import unicodedata
import uuid
from typing import List, Tuple, Optional

from rdflib.namespace import RDF

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan import model
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.harvest.interfaces import IHarvester

from ckanext.data_gov_gr.harvesters.custom_dcat_harvester import CustomDcatHarvester
from ckanext.data_gov_gr import helpers as data_gov_helpers
from ckanext.dcat.processors import RDFParser, RDFParserException
from ckanext.dcat.profiles import DCAT, DCT, FOAF

log = logging.getLogger(__name__)


class EkanDcatHarvester(CustomDcatHarvester, IHarvester):
    """
    EKAN DCAT Harvester (per-dataset DCAT-AP endpoints)

    Designed for EKAN-based portals (eg data.apdkritis.gov.gr) that expose
    DCAT-AP 2.0 per dataset but do not provide a site-wide catalog feed.

    Strategy (sequential by default):
      - Iterate NIDs 1..max_nid and probe `/dataset/<nid>/dcat-ap-2.0/xml`.
      - For HTTP 200, parse RDF/XML into CKAN dataset dict using RDFParser.
      - Store dataset JSON into HarvestObject.content and set guid from
        dataset identifier (dct:identifier). Track `guids_in_source` for deletions.

    Config (JSON on the harvest source):
      - max_nid: int (default 500)
      - nid_start: int (default 1)
      - throttle_ms: int (default 300)
    """

    def info(self):
        return {
            'name': 'ekan_dcat_harvester',
            'title': 'EKAN DCAT Harvester',
            'description': 'Harvests EKAN/DCAT datasets using per-dataset DCAT-AP endpoints',
            'form_config_interface': 'Text',
            'show_config': True,
        }

    def validate_config(self, source_config):
        if not source_config:
            return json.dumps({})
        try:
            conf = json.loads(source_config)
            if 'max_nid' in conf and not isinstance(conf['max_nid'], int):
                raise ValueError('max_nid must be integer')
            if 'nid_start' in conf and not isinstance(conf['nid_start'], int):
                raise ValueError('nid_start must be integer')
            if 'throttle_ms' in conf and not isinstance(conf['throttle_ms'], int):
                raise ValueError('throttle_ms must be integer')
            return json.dumps(conf)
        except ValueError as e:
            raise e

    def _config(self, harvest_job) -> Tuple[int, int, int]:
        conf = {}
        if harvest_job and harvest_job.source and harvest_job.source.config:
            try:
                conf = json.loads(harvest_job.source.config) or {}
            except Exception:
                conf = {}
        max_nid = int(conf.get('max_nid', 500))
        nid_start = int(conf.get('nid_start', 1))
        throttle_ms = int(conf.get('throttle_ms', 300))
        return nid_start, max_nid, throttle_ms

    def _build_dcat_url(self, base_url: str, nid: int) -> str:
        return f"{base_url.rstrip('/')}/dataset/{nid}/dcat-ap-2.0/xml"

    def _parse_single_dataset(self, rdf_xml: str) -> dict:
        """Parse an RDF/XML string into a single CKAN dataset dict.

        Returns empty dict when parsing fails or zero datasets are found.
        """
        try:
            parser = RDFParser()
            parser.parse(rdf_xml, _format='xml')
            # Expect exactly one dataset per per-dataset endpoint
            for dataset in parser.datasets():
                dataset = dataset or {}
                self._ensure_dataset_name(dataset)
                self._ensure_dataset_resources(dataset, parser)
                # Enrich resource names/descriptions using the RDF graph
                try:
                    self._enrich_resources_from_graph(dataset, parser)
                except Exception as e:
                    log.warning(f"EKAN enrichment from graph failed: {e}")
                return dataset
        except RDFParserException as e:
            log.warning(f"EKAN DCAT parse error: {e}")
        except Exception as e:
            log.warning(f"Unexpected error parsing DCAT: {e}")
        return {}

    def _ensure_dataset_name(self, dataset: dict) -> None:
        """Ensure the dataset dict contains a CKAN-safe `name`."""

        current_name = (dataset or {}).get('name')
        slug = self._slugify(current_name) if current_name else ''

        if not slug:
            candidates: List[Optional[str]] = []
            if dataset:
                candidates.extend([
                    dataset.get('identifier'),
                    dataset.get('title'),
                ])

                title_translated = dataset.get('title_translated')
                if isinstance(title_translated, dict):
                    candidates.extend([
                        title_translated.get('el'),
                        title_translated.get('en'),
                    ])

                candidates.append(dataset.get('title_translated-el'))

            for candidate in candidates:
                slug = self._slugify(candidate)
                if slug:
                    break

        if not slug:
            slug = f"dataset-{uuid.uuid4().hex[:8]}"

        dataset['name'] = slug[:100]

    def _ensure_dataset_resources(self, dataset: dict, parser: RDFParser) -> None:
        resources = dataset.get('resources') or []
        if resources:
            return

        fallback_resources = self._extract_distributions_from_graph(parser)
        if fallback_resources:
            dataset['resources'] = fallback_resources

    def _extract_distributions_from_graph(self, parser: RDFParser) -> List[dict]:
        graph = getattr(parser, 'g', None)
        if graph is None:
            return []

        distributions = list(graph.subjects(RDF.type, DCAT.Distribution))
        if not distributions:
            return []

        resources: List[dict] = []
        for idx, dist in enumerate(distributions, start=1):
            title = self._graph_literal(graph, dist, DCT.title)
            description = self._strip_html(self._graph_literal(graph, dist, DCT.description))
            access_url = self._graph_uri(graph, dist, DCAT.accessURL)
            download_url = self._graph_uri(graph, dist, DCAT.downloadURL)
            media_type = self._graph_literal(graph, dist, DCAT.mediaType)
            literal_format = self._graph_literal(graph, dist, DCT.format)
            foaf_page = self._graph_uri(graph, dist, FOAF.page)
            byte_size = self._graph_literal(graph, dist, DCAT.byteSize)

            url = download_url or access_url or foaf_page
            if not url:
                continue

            resource = {
                'url': url,
                'name': title or f'resource-{idx}',
                'distribution_ref': str(dist),
            }

            if download_url:
                resource['download_url'] = download_url
            if access_url:
                resource['access_url'] = access_url
            if description:
                resource['description'] = description
            if literal_format:
                resource['format'] = literal_format.upper()
            # Do not set 'mimetype' here; leave validation to upstream/custom mapping
            if byte_size:
                try:
                    resource['size'] = int(float(byte_size))
                except (ValueError, TypeError):
                    pass
            if foaf_page:
                resource['foaf_page'] = foaf_page

            resources.append(resource)

        return resources

    def _graph_literal(self, graph, subject, predicate) -> str:
        value = graph.value(subject, predicate)
        if value is None:
            return ''
        try:
            python_value = value.toPython()
        except Exception:
            python_value = str(value)
        if python_value is None:
            return ''
        return str(python_value).strip()

    def _graph_uri(self, graph, subject, predicate) -> str:
        value = graph.value(subject, predicate)
        if value is None:
            return ''
        return str(value).strip()

    def _strip_html(self, text: str) -> str:
        if not text:
            return ''
        cleaned = html.unescape(text)
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        return cleaned.strip()

    def _slugify(self, value: Optional[str]) -> str:
        if not value:
            return ''
        if not isinstance(value, str):
            value = str(value)

        normalized = unicodedata.normalize('NFKD', value)
        ascii_text = normalized.encode('ascii', 'ignore').decode('ascii')
        lowered = ascii_text.lower()
        slug = re.sub(r'[^a-z0-9]+', '-', lowered).strip('-')
        return slug

    def _extract_guid(self, dataset: dict) -> str:
        guid = (dataset or {}).get('identifier')
        if not guid:
            # Fallback: try uri or name
            guid = dataset.get('uri') or dataset.get('name')
        if not guid:
            # Last resort: hash of dataset JSON
            try:
                import hashlib
                guid = hashlib.sha1(json.dumps(dataset, sort_keys=True).encode('utf-8')).hexdigest()
            except Exception:
                guid = None
        return guid

    def gather_stage(self, harvest_job):
        log.info('EKAN DCAT gather started for source: %s', harvest_job.source.url)
        nid_start, max_nid, throttle_ms = self._config(harvest_job)

        # Map existing GUIDs for this source to package_ids
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(HarvestObject.current == True)
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        )
        guid_to_package_id = {guid: pkg_id for guid, pkg_id in query}

        object_ids: List[str] = []
        guids_in_source: List[str] = []

        base_url = harvest_job.source.url.rstrip('/')
        names_taken: set[str] = set()

        for nid in range(nid_start, max_nid + 1):
            dcat_url = self._build_dcat_url(base_url, nid)
            content, content_type = self._get_content_and_type(dcat_url, harvest_job, page=1, content_type='application/rdf+xml')
            if not content:
                # Most likely 404 or other error
                time.sleep(throttle_ms / 1000.0)
                continue

            dataset = self._parse_single_dataset(content)
            if not dataset:
                time.sleep(throttle_ms / 1000.0)
                continue

            # Ensure owner_org is set from source (EKAN gather bypasses default DCAT gather)
            self._ensure_owner_org(dataset, harvest_job)

            # Ensure name has minimum length and is unique in this run and DB
            self._ensure_unique_name(dataset, names_taken)

            # EKAN-specific normalizations
            self._normalize_frequency(dataset)
            self._normalize_resource_formats(dataset)

            guid = self._extract_guid(dataset)
            if not guid:
                log.warning('Skipping dataset without GUID (nid=%s)', nid)
                time.sleep(throttle_ms / 1000.0)
                continue

            guids_in_source.append(guid)

            # Serialize dataset dict to JSON for import_stage
            as_string = json.dumps(dataset)

            try:
                if guid in guid_to_package_id:
                    obj = HarvestObject(
                        guid=guid,
                        job=harvest_job,
                        package_id=guid_to_package_id[guid],
                        content=as_string,
                        extras=[HarvestObjectExtra(key='status', value='change')],
                    )
                else:
                    obj = HarvestObject(
                        guid=guid,
                        job=harvest_job,
                        content=as_string,
                        extras=[HarvestObjectExtra(key='status', value='new')],
                    )
                obj.save()
                object_ids.append(obj.id)
            except Exception as e:
                log.error('Error creating HarvestObject for nid=%s: %s', nid, e)

            # Throttle between requests
            time.sleep(throttle_ms / 1000.0)

        # Mark deletions
        try:
            to_delete_ids = self._mark_datasets_for_deletion(guids_in_source, harvest_job)
            object_ids.extend(to_delete_ids)
        except Exception as e:
            log.warning('Error computing deletions: %s', e)

        log.info('EKAN DCAT gather completed: %d objects (+%d deletions)', len(object_ids), max(0, len(object_ids) - len(guids_in_source)))
        return object_ids

    def fetch_stage(self, harvest_object):
        # We fetched DCAT content in gather_stage and stored JSON dataset dict
        return True

    def modify_package_dict(self, package_dict, temp_dict, harvest_object):
        """
        Run parent custom mapping, then apply EKAN-specific post-normalization for resources.
        This ensures we don't leave invalid resource formats (eg 'Unknown') that violate
        the controlled vocabulary on data.gov.gr, and removes helper keys not in schema.
        """
        out = super(EkanDcatHarvester, self).modify_package_dict(package_dict, temp_dict, harvest_object)

        try:
            # EKAN datasets are always considered PUBLIC for access_rights
            self._force_public_access_rights(out)
            # Ensure applicable_legislation is set for PUBLIC datasets
            self._ensure_applicable_legislation(out)

            # Remove helper keys not guaranteed by schema and strip placeholder formats
            resources = out.get('resources') or []
            for res in list(resources):
                if not isinstance(res, dict):
                    continue
                # Drop helper keys that may not exist in the schema
                for k in ('distribution_ref', 'foaf_page'):
                    if k in res:
                        res.pop(k, None)
                # Remove placeholder/invalid format values
                if isinstance(res.get('format'), str) and res['format'].strip().lower() in ('unknown', 'n/a', 'na', 'none'):
                    res.pop('format', None)

            # Re-run EKAN resource format normalization with controlled vocabulary
            self._normalize_resource_formats(out)

            # Final safety net: drop resources without a valid URL
            filtered = []
            for res in resources:
                try:
                    url = res.get('url')
                    if self._looks_like_url(url):
                        filtered.append(res)
                except Exception:
                    continue
            if len(filtered) != len(resources):
                out['resources'] = filtered
        except Exception as e:
            log.warning('EKAN post-normalization failed: %s', e)

        return out

    def _ensure_applicable_legislation(self, dataset: dict) -> None:
        """
        Ensure that the dataset has an ``applicable_legislation`` field set
        when access_rights is PUBLIC.

        Uses the same configuration keys as the manual UI:
        - ``ckanext.data_gov_gr.dataset.legislation.open`` for open datasets.
        """
        try:
            if not isinstance(dataset, dict):
                return

            existing = dataset.get('applicable_legislation')
            if existing:
                return

            access_rights = dataset.get('access_rights')
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

            dataset['applicable_legislation'] = [value]
        except Exception as e:
            log.warning('EKAN applicable_legislation PUBLIC normalization failed: %s', e)

    def _force_public_access_rights(self, dataset: dict) -> None:
        """Force access_rights to PUBLIC for EKAN-harvested datasets.

        Uses the Publications Office authority URI. Also removes any
        access_rights occurrences from extras to avoid conflicts with
        scheming validation.
        """
        try:
            public_uri = 'http://publications.europa.eu/resource/authority/access-right/PUBLIC'
            dataset['access_rights'] = public_uri

            extras = dataset.get('extras')
            if isinstance(extras, list):
                dataset['extras'] = [
                    e for e in extras
                    if not (
                        isinstance(e, dict)
                        and (e.get('key') or '').strip().lower() == 'access_rights'
                    )
                ]
        except Exception as e:
            log.warning('EKAN access_rights PUBLIC normalization failed: %s', e)

    def _ensure_owner_org(self, dataset: dict, harvest_job) -> None:
        try:
            if dataset.get('owner_org'):
                log.info('[EKAN OWNER_ORG] Dataset already has owner_org=%s', dataset.get('owner_org'))
                return
            # Match DCATRDFHarvester: read owner_org from the harvest source dataset
            try:
                source_pkg = model.Package.get(harvest_job.source.id)
                if source_pkg and source_pkg.owner_org:
                    dataset['owner_org'] = source_pkg.owner_org
                    log.info('[EKAN OWNER_ORG] Set owner_org from source package: %s', source_pkg.owner_org)
                    return
            except Exception:
                pass
            # Fallback: use owner_org on harvest source row, if present
            source_owner = getattr(getattr(harvest_job, 'source', None), 'owner_org', None)
            if source_owner:
                dataset['owner_org'] = source_owner
                log.info('[EKAN OWNER_ORG] Set owner_org from harvest source row: %s', source_owner)
                return
        except Exception as e:
            log.warning('Could not assign owner_org during EKAN gather: %s', e)

    def _normalize_frequency(self, dataset: dict) -> None:
        """Map ISO repeatable intervals to EU Frequency codes before shared processing."""
        val = dataset.get('frequency')
        if not isinstance(val, str):
            return
        iso_map = {
            'R/PT1H': 'HOURLY', 'PT1H': 'HOURLY',
            'P1D': 'DAILY', 'R/P1D': 'DAILY',
            'P7D': 'WEEKLY', 'P1W': 'WEEKLY', 'R/P1W': 'WEEKLY',
            'P1M': 'MONTHLY', 'R/P1M': 'MONTHLY',
            'P1Y': 'ANNUAL', 'R/P1Y': 'ANNUAL',
        }
        up = val.strip().upper()
        if up in iso_map:
            dataset['frequency'] = iso_map[up]

    def _normalize_resource_formats(self, dataset: dict) -> None:
        resources = dataset.get('resources') or []
        if not resources:
            return
        # Load controlled vocabulary codes via shared helper on base class
        try:
            valid_codes = self._get_vocabulary_valid_codes('Machine Readable File Format')
        except Exception:
            valid_codes = set()
        try:
            media_valid_codes = self._get_vocabulary_valid_codes('Media types')
        except Exception:
            media_valid_codes = set()
        try:
            media_uri_map = self._get_vocabulary_uri_map('Media types')
        except Exception:
            media_uri_map = {}

        mime_to_code = {
            'text/csv': 'CSV',
            'application/json': 'JSON',
            'application/geo+json': 'GEOJSON',
            'application/xml': 'XML',
            'text/xml': 'XML',
            'application/pdf': 'PDF',
            'text/html': 'HTML',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'XLSX',
            'application/vnd.ms-excel': 'XLS',
            'application/zip': 'ZIP',
            'application/vnd.google-earth.kml+xml': 'KML',
            'application/vnd.google-earth.kmz': 'KMZ',
            'image/jpeg': 'JPEG',
            'image/jpg': 'JPEG',
            'image/png': 'PNG',
            'image/tiff': 'TIFF',
            'image/gif': 'GIF',
            'image/webp': 'WEBP',
            'text/plain': 'TXT',
            'text/tab-separated-values': 'TSV',
            'application/vnd.ms-excel.sheet.macroenabled.12': 'XLSM',
            'application/vnd.ms-excel.sheet.binary.macroenabled.12': 'XLSB',
            'application/vnd.oasis.opendocument.spreadsheet': 'ODS',
            'application/vnd.oasis.opendocument.text': 'ODT',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'PPTX',
            'application/vnd.ms-powerpoint': 'PPT',
            'application/msword': 'DOC',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'DOCX',
            'application/vnd.parquet': 'PARQUET',
            'application/ld+json': 'JSON_LD',
        }
        # Inverse mapping: code -> canonical IANA mimetype token
        code_to_mime = {
            'CSV': 'text/csv',
            'JSON': 'application/json',
            'GEOJSON': 'application/geo+json',
            'XML': 'application/xml',
            'HTML': 'text/html',
            'PDF': 'application/pdf',
            'XLSX': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'XLS': 'application/vnd.ms-excel',
            'ZIP': 'application/zip',
            'KML': 'application/vnd.google-earth.kml+xml',
            'KMZ': 'application/vnd.google-earth.kmz',
            'JPEG': 'image/jpeg',
            'PNG': 'image/png',
            'TIFF': 'image/tiff',
            'GIF': 'image/gif',
            'WEBP': 'image/webp',
            'TXT': 'text/plain',
            'TSV': 'text/tab-separated-values',
            'XLSM': 'application/vnd.ms-excel.sheet.macroenabled.12',
            'XLSB': 'application/vnd.ms-excel.sheet.binary.macroenabled.12',
            'ODS': 'application/vnd.oasis.opendocument.spreadsheet',
            'ODT': 'application/vnd.oasis.opendocument.text',
            'PPTX': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'PPT': 'application/vnd.ms-powerpoint',
            'DOC': 'application/msword',
            'DOCX': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'PARQUET': 'application/vnd.parquet',
            'JSON_LD': 'application/ld+json',
        }
        ext_to_code = {
            '.csv': 'CSV', '.tsv': 'TSV', '.txt': 'TXT',
            '.json': 'JSON', '.jsonld': 'JSON_LD', '.geojson': 'GEOJSON',
            '.xml': 'XML', '.rdf': 'RDF', '.ttl': 'RDF_TURTLE', '.nt': 'RDF_N_TRIPLES', '.trig': 'RDF_TRIG', '.trix': 'RDF_TRIX', '.n3': 'N3',
            '.pdf': 'PDF', '.pdfa': 'PDFA1A',  # generic pdfa fallback
            '.html': 'HTML', '.htm': 'HTML', '.xhtml': 'XHTML',
            '.xlsx': 'XLSX', '.xls': 'XLS', '.xlsm': 'XLSM', '.xlsb': 'XLSB',
            '.zip': 'ZIP', '.rar': 'RAR', '.7z': '7Z', '.gz': 'GZIP', '.xz': 'XZ', '.tar': 'TAR', '.tar.gz': 'TAR_GZ', '.tar.xz': 'TAR_XZ',
            '.kml': 'KML', '.kmz': 'KMZ',
            '.jpg': 'JPEG', '.jpeg': 'JPEG', '.png': 'PNG', '.gif': 'GIF', '.tif': 'TIFF', '.tiff': 'TIFF', '.webp': 'WEBP',
            '.shp': 'SHP', '.dbf': 'DBF', '.gml': 'GML', '.svg': 'SVG', '.gpx': 'GPX', '.gpkg': 'GPKG', '.parquet': 'PARQUET',
            '.doc': 'DOC', '.docx': 'DOCX', '.ppt': 'PPT', '.pptx': 'PPTX', '.odt': 'ODT', '.ods': 'ODS', '.rtf': 'RTF'
        }
        def service_code_from_url(url: str) -> str:
            if not url or not isinstance(url, str):
                return ''
            low = url.lower()
            # Common OGC services detection
            if 'service=wms' in low or 'request=getcapabilities' in low and 'wms' in low:
                return 'WMS_SRVC'
            if 'service=wfs' in low or 'wfs?' in low:
                return 'WFS_SRVC'
            if 'service=wmts' in low or 'wmts?' in low:
                return 'WMTS_SRVC'
            if 'service=wcs' in low or 'wcs?' in low:
                return 'WCS_SRVC'
            # ArcGIS REST pattern
            if '/arcgis/rest/services' in low:
                return 'MAP_SRVC'
            return ''
        def code_from_url(url: str) -> str:
            if not url or not isinstance(url, str):
                return ''
            low = url.lower()
            for ext, code in ext_to_code.items():
                if low.endswith(ext):
                    return code
            return ''

        placeholder_formats = {
            '', 'UNKNOWN', 'UNK', 'N/A', 'NA', 'NONE', 'OTHER',
            'APPLICATION/OCTET-STREAM', 'OCTET-STREAM', 'BINARY'
        }

        for res in resources:
            if not isinstance(res, dict):
                continue
            self._normalize_single_resource_format(
                res,
                valid_codes,
                mime_to_code,
                ext_to_code,
                service_code_from_url,
                code_from_url,
                placeholder_formats,
            )
            self._normalize_single_resource_mimetype(
                res,
                dataset,
                media_valid_codes,
                media_uri_map,
                mime_to_code,
                code_to_mime,
                code_from_url,
            )

    def _normalize_single_resource_format(
        self,
        res: dict,
        valid_codes: set,
        mime_to_code: dict,
        ext_to_code: dict,
        service_code_from_url,
        code_from_url,
        placeholder_formats: set,
    ) -> None:
        fmt = res.get('format')
        existing = ''
        if isinstance(fmt, str):
            existing = self._extract_code_from_identifier(fmt).upper().strip()

        # Remove placeholders early
        if existing in placeholder_formats:
            existing = ''

        # Build a candidate from mimetype or URL
        candidate = existing
        if not candidate:
            mt = res.get('mimetype')
            if isinstance(mt, str) and mt.strip():
                candidate = mime_to_code.get(mt.strip().lower(), '')
        if not candidate:
            candidate = (
                code_from_url(res.get('url') or '') or
                code_from_url(res.get('download_url') or '') or
                code_from_url(res.get('access_url') or '')
            )
        if not candidate:
            # Try service endpoint detection
            candidate = (
                service_code_from_url(res.get('url') or '') or
                service_code_from_url(res.get('download_url') or '') or
                service_code_from_url(res.get('access_url') or '')
            )

        # Only set format if it’s in the vocabulary. Otherwise, remove it to avoid validation errors.
        if valid_codes:
            if candidate and candidate in valid_codes:
                res['format'] = candidate
            else:
                if 'format' in res:
                    del res['format']
        else:
            # No vocabulary available: be conservative and drop dubious values
            if existing:
                # Keep existing only if it matches a conservative whitelist
                conservative = {'CSV', 'JSON', 'XML', 'XLS', 'XLSX', 'HTML', 'PDF', 'ZIP', 'KML', 'GEOJSON'}
                if existing in conservative:
                    res['format'] = existing
                else:
                    if 'format' in res:
                        del res['format']
            else:
                if 'format' in res:
                    del res['format']

    def _normalize_single_resource_mimetype(
        self,
        res: dict,
        dataset: dict,
        media_valid_codes: set,
        media_uri_map: dict,
        mime_to_code: dict,
        code_to_mime: dict,
        code_from_url,
    ) -> None:
        # Also set a valid mimetype code when possible (kept minimal & vocab-driven)
        # Prefer mapping from explicit mimetype value; fallback to URL-derived code
        media_candidate = ''  # normalized CODE (eg. CSV, ZIP)
        media_token = ''      # canonical token (eg. text/csv, application/zip)
        mt_val = res.get('mimetype')
        if isinstance(mt_val, str) and mt_val.strip():
            # Accept: full mimetype (text/csv), short code (csv/CSV), or authority URI
            value = mt_val.strip()
            # If it's a full token, map to code; if URI, extract last segment; else treat as code
            media_candidate = mime_to_code.get(value.lower(), '')
            if not media_candidate:
                media_candidate = (self._extract_code_from_identifier(value) or '').upper()
            # Also preserve canonical token from user value when possible
            if '/' in value and '://' not in value:
                media_token = value.lower()
        # Consider format hint
        if not media_candidate:
            fmt_val = res.get('format')
            if isinstance(fmt_val, str) and fmt_val.strip():
                media_candidate = (self._extract_code_from_identifier(fmt_val) or '').upper()
        # Consider URL extension
        if not media_candidate:
            media_candidate = (
                code_from_url(res.get('url') or '') or
                code_from_url(res.get('download_url') or '') or
                code_from_url(res.get('access_url') or '')
            )
        # Derive canonical token if not provided
        if not media_token and media_candidate:
            media_token = code_to_mime.get(media_candidate, '')

        # Try to resolve to authoritative URI using vocabulary mapping
        uri = None
        if media_token:
            uri = media_uri_map.get(media_token.upper())
        if not uri and media_candidate:
            uri = media_uri_map.get(media_candidate)
        # Fallback: construct canonical IANA URL (preferred by site vocabulary)
        if not uri and media_token:
            uri = f"https://www.iana.org/assignments/media-types/{media_token.lower()}"

        if uri:
            res['mimetype'] = uri
        else:
            # Drop invalid mimetype values to avoid schema rejections
            if 'mimetype' in res:
                del res['mimetype']
            # Preserve information at dataset-level extras (non-blocking)
            if media_candidate:
                self._add_dataset_extra(dataset, 'mimetype_fallback', media_candidate.lower())

    def _add_dataset_extra(self, dataset: dict, key: str, value: str) -> None:
        try:
            if not key or value is None:
                return
            extras = dataset.setdefault('extras', [])
            # Avoid duplicate key-value pairs
            for e in extras:
                if isinstance(e, dict) and e.get('key') == key and str(e.get('value')) == str(value):
                    return
            extras.append({'key': key, 'value': value})
        except Exception:
            # Non-fatal: extras recording is best-effort
            pass

    def _get_vocabulary_uri_map(self, vocabulary_name: str) -> dict:
        """Return mapping CODE -> value_uri for a given controlled vocabulary.

        The mapping uses the same alias resolution as the base class, and
        extracts codes from `value_uri` using `_extract_code_from_identifier`.
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
            return {}
        mapping = {}
        for tag in tags:
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

    def _looks_like_url(self, value: Optional[str]) -> bool:
        if not value or not isinstance(value, str):
            return False
        v = value.strip().lower()
        return v.startswith('http://') or v.startswith('https://') or v.startswith('ftp://')

    def _first_valid_url(self, *candidates: Optional[str]) -> str:
        for c in candidates:
            if self._looks_like_url(c):
                return c  # type: ignore[return-value]
        return ''

    def _enrich_resources_from_graph(self, dataset: dict, parser: RDFParser) -> None:
        """Use Distribution titles/descriptions from the RDF graph to improve resource names/descriptions.

        Many EKAN distributions expose only resource slugs (e.g. numeric ids or URL segments).
        If the parsed resource name is missing or looks like a placeholder, replace it with
        the Distribution dct:title from the RDF graph. Also fill empty descriptions from
        dct:description when available.
        """
        resources = dataset.get('resources') or []
        if not resources:
            return
        graph = getattr(parser, 'g', None)
        if graph is None:
            return

        # Build maps for matching even when Distributions are blank nodes
        dist_map: dict[str, dict] = {}
        url_map: dict[str, dict] = {}
        for dist in graph.subjects(RDF.type, DCAT.Distribution):
            ref = str(dist)
            title = self._graph_literal(graph, dist, DCT.title)
            desc = self._strip_html(self._graph_literal(graph, dist, DCT.description))
            media_type = self._graph_literal(graph, dist, DCAT.mediaType)
            literal_format = self._graph_literal(graph, dist, DCT.format)
            dl_url = self._graph_uri(graph, dist, DCAT.downloadURL)
            ac_url = self._graph_uri(graph, dist, DCAT.accessURL)
            page_url = self._graph_uri(graph, dist, FOAF.page)
            info = {
                'title': title,
                'description': desc,
                'media_type': media_type,
                'literal_format': literal_format,
                'download_url': dl_url,
                'access_url': ac_url,
                'foaf_page': page_url,
            }
            dist_map[ref] = info

            # Index by URLs (downloadURL, accessURL, foaf:page)
            for u in (dl_url, ac_url, page_url):
                if u:
                    url_map[u.strip().lower().rstrip('/')] = info

        def looks_placeholder(name: str, res: dict) -> bool:
            if not name or len(name.strip()) < 3:
                return True
            n = name.strip().lower()
            # Pure digits or equals last segment of url/uri
            if n.isdigit():
                return True
            last = ''
            for key in ('url', 'uri', 'distribution_ref'):
                val = res.get(key)
                if isinstance(val, str) and val:
                    seg = val.rstrip('/').split('/')[-1].lower()
                    if seg:
                        last = seg
                        break
            if last and (n == last):
                return True
            return False

        # Prepare a robust fallback in case we can't match anything
        fallback_from_graph = self._extract_distributions_from_graph(parser)
        matched = 0

        for res in resources:
            if not isinstance(res, dict):
                continue
            ref = res.get('distribution_ref') or res.get('uri')
            info = dist_map.get(ref) if isinstance(ref, str) else None
            if not info:
                # Try URL-based matching when subjects are blank nodes or URIs differ
                for key in ('url', 'download_url', 'access_url', 'uri', 'foaf_page'):
                    v = res.get(key)
                    if isinstance(v, str) and v:
                        info = url_map.get(v.strip().lower().rstrip('/'))
                        if info:
                            break
            # Backfill URL from known candidates when missing
            if not self._looks_like_url(res.get('url')):
                backfilled_url = self._first_valid_url(
                    # Resource-provided URLs
                    res.get('download_url'), res.get('access_url'),
                    # Graph-provided URLs for the matched distribution
                    info.get('download_url') if info else None,
                    info.get('access_url') if info else None,
                    info.get('foaf_page') if info else None,
                    # Fallbacks frequently present in harvested payloads
                    res.get('uri'), res.get('distribution_ref'),
                )
                if backfilled_url:
                    res['url'] = backfilled_url

            # Improve name
            name = res.get('name') if isinstance(res.get('name'), str) else ''
            if info and looks_placeholder(name, res) and info.get('title'):
                res['name'] = info['title'][:100]
                matched += 1
            # Improve description
            if info and (not res.get('description')) and info.get('description'):
                res['description'] = info['description']
                matched += 1
            # Avoid setting 'mimetype' here; format normalization works from URL as well
            # If format is missing or looks unusable, carry literal_format for normalization
            if info and (not res.get('format') or str(res.get('format')).strip().lower() in ('', 'unknown')) and info.get('literal_format'):
                res['format'] = str(info['literal_format']).upper()

        # If nothing matched and we do have distributions extracted from the graph,
        # replace resources entirely to capture proper titles/URLs from the graph.
        if matched == 0 and fallback_from_graph:
            dataset['resources'] = fallback_from_graph

    def _name_exists(self, name: str) -> bool:
        try:
            if not name:
                return False
            exists = model.Session.query(model.Package.id).filter(model.Package.name == name).first()
            return bool(exists)
        except Exception:
            return False

    def _ensure_unique_name(self, dataset: dict, names_taken: set) -> None:
        name = (dataset or {}).get('name') or ''
        # Enforce minimum length of 2
        if len(name) < 2:
            base = name if name else 'ds'
            name = f"{base}-x"

        base = name
        suffix = 1
        # Avoid duplicates within this gather and existing packages
        while name in names_taken or self._name_exists(name):
            suffix += 1
            name = f"{base}-{suffix}"
            if len(name) > 100:
                name = name[:100]
                base = name
        dataset['name'] = name
        names_taken.add(name)
