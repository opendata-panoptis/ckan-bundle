# -*- coding: utf-8 -*-
import logging
import json
import re
import hashlib
from collections import defaultdict

from ckan import model
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.dcat.processors import RDFParser, RDFParserException
import ckan.plugins as p
from ckanext.dcat.interfaces import IDCATRDFHarvester
from rdflib import Namespace
from rdflib.namespace import RDF

from .custom_dcat_harvester import CustomDcatHarvester

log = logging.getLogger(__name__)


class BankOfGreeceHarvester(CustomDcatHarvester):
    """
    Custom Harvester specifically for Bank of Greece.

    Key features:
    - Merges Greek (el) and English (en) entries into single multilingual datasets
    - Inherits validation fixes from CustomDcatHarvester
    - Forces ECON theme for all datasets
    - Custom frequency and license mapping
    """

    def info(self):
        return {
            'name': 'bank_of_greece_harvester',
            'title': 'Bank of Greece DCAT Harvester',
            'description': 'Harvester for BoG with multilingual merging, forced ECON theme',
            'form_config_interface': 'Text',
            'show_config': False
        }

    def validate_config(self, source_config):
        """
        Ensure a sane default RDF format for BoG sources.

        BoG endpoints typically return RDF/XML but may use a generic
        "text/xml" content-type, which rdflib does not recognize as a
        registered RDF parser format. By forcing rdf_format="xml" when
        unset, we avoid "No plugin registered for (text/xml, ...)" errors.
        """
        if not source_config:
            return json.dumps({'rdf_format': 'xml'})

        try:
            conf = json.loads(source_config) or {}
        except ValueError:
            return super().validate_config(source_config)

        if not conf.get('rdf_format'):
            conf['rdf_format'] = 'xml'

        return super().validate_config(json.dumps(conf))

    def _extract_dataset_id_from_guid(self, guid):
        """
        Extract the numeric dataset ID from a BoG GUID.

        Examples:
            https://opendata.bankofgreece.gr/el/dataset/25 -> 25
            https://opendata.bankofgreece.gr/en/dataset/25/ -> 25
            .../dataset/25?foo=bar -> 25

        Returns None if pattern doesn't match.
        """
        if not guid or not isinstance(guid, str):
            return None

        # Robust regex: allow optional trailing slash, ignore query/fragment
        # Matches .../(el or en)/dataset/(digits) ...
        match = re.search(r'/(?:el|en)/dataset/(\d+)(?:/|[?#]|$)', guid)
        if match:
            return match.group(1)
        return None

    def _extract_language_from_guid(self, guid):
        """
        Extract the language code from a BoG GUID.

        Examples:
            https://opendata.bankofgreece.gr/el/dataset/25 -> el
            https://opendata.bankofgreece.gr/en/dataset/25 -> en

        Returns None if pattern doesn't match.
        """
        if not guid or not isinstance(guid, str):
            return None

        match = re.search(r'/(el|en)/dataset/\d+(?:/|[?#]|$)', guid)
        if match:
            return match.group(1)
        return None

    def _merge_multilingual_datasets(self, datasets):
        """
        Merge Greek and English versions of the same dataset into one multilingual entry.

        The BoG feed provides separate entries for /el/dataset/X and /en/dataset/X.
        This method:
        1. Groups datasets by their numeric ID
        2. Merges title and notes into title_translated and notes_translated
        3. Uses English title for the 'name' field (better slug generation)
        4. Keeps resources from all versions (deduped by URL)
        5. Merges tags from all versions

        Returns a list of merged datasets.
        """
        # Group datasets by their base ID
        grouped = defaultdict(list)
        unmatched_counter = 0

        for dataset in datasets:
            guid = self._get_dict_value(dataset, 'uri') or self._get_dict_value(dataset, 'identifier')
            dataset_id = self._extract_dataset_id_from_guid(guid)

            if dataset_id:
                lang = self._extract_language_from_guid(guid)
                grouped[dataset_id].append((lang, dataset, guid))
            else:
                # Dataset doesn't match our pattern, keep as-is with unique key
                unmatched_counter += 1
                unique_key = f"_single_{unmatched_counter}_{guid}"
                grouped[unique_key].append((None, dataset, guid))

        merged_datasets = []

        for dataset_id, entries in grouped.items():
            if len(entries) == 1:
                # Single entry, just use it
                lang, dataset, guid = entries[0]
                log.info(f"[BoG] Single entry for dataset {dataset_id}: {guid}")
                merged_datasets.append(dataset)
                continue

            # Multiple entries - merge them
            log.info(f"[BoG] Merging {len(entries)} entries for dataset {dataset_id}")

            # Sort so English comes first (for name generation priority)
            entries_sorted = sorted(entries, key=lambda x: (x[0] != 'en', x[0] or ''))

            # Use the first (English if available) as the base
            base_lang, base_dataset, base_guid = entries_sorted[0]
            merged = dict(base_dataset)

            # Initialize translated fields
            title_translated = {}
            notes_translated = {}

            # Track resources by URL to avoid duplicates
            resources_by_url = {}

            # Collect all themes from all language versions
            all_themes = []

            # Collect frequency (should be the same across languages, but take first found)
            collected_frequency = None

            # Collect landing pages per language
            landing_pages = {}

            # Collect BoG theme labels from all language versions
            merged_theme_labels = []

            # Collect tags from all language versions
            all_tags_map = {}  # name -> dict

            for lang, dataset, guid in entries:
                # Collect titles
                title = dataset.get('title')
                if title and lang:
                    title_translated[lang] = title
                elif title and not lang:
                    title_translated['en'] = title

                # Collect notes/descriptions
                notes = dataset.get('notes') or dataset.get('description')
                if notes and lang:
                    notes_translated[lang] = notes
                elif notes and not lang:
                    notes_translated['en'] = notes

                # Collect resources (dedup by URL)
                for res in dataset.get('resources', []):
                    url = res.get('url')
                    if url and url not in resources_by_url:
                        resources_by_url[url] = res

                # Collect themes from all language versions
                dataset_themes = dataset.get('theme', [])
                if isinstance(dataset_themes, str):
                    dataset_themes = [dataset_themes]
                for theme in dataset_themes:
                    if theme and theme not in all_themes:
                        all_themes.append(theme)

                # Collect frequency (take first non-empty value found)
                if not collected_frequency:
                    freq = dataset.get('frequency') or dataset.get('accrualPeriodicity')
                    if freq:
                        collected_frequency = freq

                # Collect landing page per language
                landing = dataset.get('landing_page') or dataset.get('landingPage') or dataset.get('url')
                if landing and lang:
                    landing_pages[lang] = landing
                elif landing and not landing_pages:
                    landing_pages['en'] = landing  # Default to 'en' key

                # Collect BoG-specific theme labels from extras (if present)
                for extra in (dataset.get('extras') or []):
                    if not isinstance(extra, dict):
                        continue
                    if extra.get('key') != 'bog_theme_labels':
                        continue
                    raw_labels = extra.get('value')
                    labels = []
                    if isinstance(raw_labels, str):
                        try:
                            labels = json.loads(raw_labels)
                        except Exception:
                            labels = []
                    elif isinstance(raw_labels, list):
                        labels = raw_labels
                    for lbl in labels:
                        if isinstance(lbl, str):
                            clean = lbl.strip()
                            if clean and clean not in merged_theme_labels:
                                merged_theme_labels.append(clean)

                # Collect tags (keywords) from all versions
                for tag in dataset.get('tags', []):
                    if isinstance(tag, dict):
                        t_name = tag.get('name', '').strip()
                        if t_name and t_name not in all_tags_map:
                            all_tags_map[t_name] = tag

                log.debug(f"[BoG] Merged {lang} entry: title='{title[:50] if title else 'N/A'}...'")

            # Set translated fields
            if title_translated:
                merged['title_translated'] = title_translated
                # Use English title for main title if available, otherwise Greek
                merged['title'] = title_translated.get('en') or title_translated.get('el') or merged.get('title', '')

            if notes_translated:
                merged['notes_translated'] = notes_translated
                merged['notes'] = notes_translated.get('en') or notes_translated.get('el') or merged.get('notes', '')

            # Set merged resources
            merged['resources'] = list(resources_by_url.values())

            # Set merged themes (from all language versions)
            if all_themes:
                merged['theme'] = all_themes
                log.info(f"[BoG] Collected {len(all_themes)} themes from all language versions")

            # Set merged tags
            if all_tags_map:
                merged['tags'] = list(all_tags_map.values())
                log.info(f"[BoG] Collected {len(all_tags_map)} distinct tags from all language versions")

            # Set frequency
            if collected_frequency:
                merged['frequency'] = collected_frequency
                log.debug(f"[BoG] Set frequency: {collected_frequency}")

            # Set landing page (prefer Greek)
            if landing_pages:
                primary_landing = landing_pages.get('el') or landing_pages.get('en') or next(iter(landing_pages.values()))
                merged['landing_page'] = primary_landing
                log.debug(f"[BoG] Set landing_page: {primary_landing}")

            # Attach merged BoG theme labels to extras so we can turn them into tags later
            if merged_theme_labels:
                # Ensure we only keep a single bog_theme_labels extra to avoid duplicate-key errors
                base_extras = merged.get('extras') or []
                cleaned_extras = [
                    e for e in base_extras
                    if not (isinstance(e, dict) and e.get('key') == 'bog_theme_labels')
                ]
                cleaned_extras.append({
                    'key': 'bog_theme_labels',
                    'value': json.dumps(merged_theme_labels)
                })
                merged['extras'] = cleaned_extras

            # Use a canonical GUID (prefer English version)
            en_guid = None
            el_guid = None
            for lang, dataset, guid in entries:
                if lang == 'en':
                    en_guid = guid
                elif lang == 'el':
                    el_guid = guid

            canonical_guid = en_guid or el_guid or base_guid

            # Update URI/identifier to canonical
            merged['uri'] = canonical_guid
            for extra in merged.get('extras', []):
                if extra.get('key') == 'uri':
                    extra['value'] = canonical_guid

            # Store source languages info in extras
            source_langs = [e[0] for e in entries if e[0]]
            merged.setdefault('extras', []).append({
                'key': 'source_languages',
                'value': json.dumps(source_langs)
            })

            log.info(f"[BoG] Merged dataset {dataset_id}: '{merged.get('title', 'N/A')[:50]}' with {len(merged.get('resources', []))} resources")
            merged_datasets.append(merged)

        return merged_datasets

    def _collect_theme_labels_from_graph(self, parser):
        """
        Collect skos:prefLabel values (all languages) for each DCAT dataset
        in the RDF graph, keyed by dataset URI.

        This is BoG-specific logic used to preserve human-readable category
        names (both el/en) as tags, without changing global DCAT parsing.
        """
        try:
            graph = parser.g
        except Exception:
            return {}

        DCAT = Namespace("http://www.w3.org/ns/dcat#")
        SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

        labels_by_dataset = {}

        try:
            for dataset_ref in graph.subjects(RDF.type, DCAT.Dataset):
                dataset_uri = str(dataset_ref)
                labels = []

                for theme_ref in graph.objects(dataset_ref, DCAT.theme):
                    for label_node in graph.objects(theme_ref, SKOS.prefLabel):
                        value = str(label_node).strip()
                        if value and value not in labels:
                            labels.append(value)

                if labels:
                    labels_by_dataset[dataset_uri] = labels
        except Exception:
            log.warning("[BoG] Failed to collect theme labels from graph", exc_info=True)
            # Fail-safe: if anything goes wrong, just return what we have
            return labels_by_dataset

        return labels_by_dataset

    def gather_stage(self, harvest_job):
        """
        Override gather_stage to merge multilingual entries from BoG.

        This method:
        1. Fetches and parses the RDF content (using parent logic)
        2. Groups datasets by their base ID (ignoring /el/ vs /en/)
        3. Merges Greek and English versions into single multilingual datasets
        4. Creates HarvestObjects for the merged datasets
        """
        log.info('[BoG] Starting gather_stage with multilingual merging')

        rdf_format = None
        max_pages = None
        current_page = 1

        if harvest_job.source.config:
            config = json.loads(harvest_job.source.config)
            rdf_format = config.get("rdf_format")
            max_pages = config.get("max_pages", None)

        next_page_url = harvest_job.source.url
        all_datasets = []
        guids_in_source = []
        object_ids = []
        last_content_hash = None
        self._names_taken = []

        while next_page_url:
            if max_pages and current_page > max_pages:
                log.info('[BoG] Reached maximum page limit of %d pages' % max_pages)
                break
            current_page += 1

            # Hook: before_download
            for harvester in p.PluginImplementations(IDCATRDFHarvester):
                next_page_url, before_download_errors = harvester.before_download(next_page_url, harvest_job)
                for error_msg in before_download_errors:
                    self._save_gather_error(error_msg, harvest_job)
                if not next_page_url:
                    return []

            try:
                content, rdf_format = self._get_content_and_type(next_page_url, harvest_job, 1, content_type=rdf_format)
            except Exception as e:
                error_msg = f'[BoG] Failed to get content from {next_page_url}: {e}'
                log.error(error_msg)
                self._save_gather_error(error_msg, harvest_job)
                break

            content_hash = hashlib.md5()
            if content:
                content_hash.update(content.encode('utf8'))

            if last_content_hash:
                if content_hash.digest() == last_content_hash.digest():
                    log.warning('[BoG] Remote content was the same even when using a paginated URL, skipping')
                    break
            
            # Update hash for next iteration to detect if page N+1 is same as page N
            last_content_hash = content_hash

            # Hook: after_download
            for harvester in p.PluginImplementations(IDCATRDFHarvester):
                content, after_download_errors = harvester.after_download(content, harvest_job)
                for error_msg in after_download_errors:
                    self._save_gather_error(error_msg, harvest_job)

            if not content:
                return []

            # Check for HTML error page
            if content.strip().startswith('<!DOCTYPE html>') or '<title>Error' in content:
                error_msg = f'[BoG] Received HTML error page instead of RDF content from {next_page_url}'
                log.error(error_msg)
                self._save_gather_error(error_msg, harvest_job)
                break

            # Parse RDF
            parser = RDFParser()
            try:
                parser.parse(content, _format=rdf_format)
            except RDFParserException as e:
                self._save_gather_error(f'[BoG] Error parsing the RDF file: {e}', harvest_job)
                return []

            # Hook: after_parsing
            for harvester in p.PluginImplementations(IDCATRDFHarvester):
                parser, after_parsing_errors = harvester.after_parsing(parser, harvest_job)
                for error_msg in after_parsing_errors:
                    self._save_gather_error(error_msg, harvest_job)

            if not parser:
                return []

            # BoG-specific: collect skos:prefLabel theme labels per dataset URI
            theme_labels_by_uri = self._collect_theme_labels_from_graph(parser)

            # Collect all datasets from this page, attaching BoG theme labels
            for dataset in parser.datasets():
                dataset_uri = self._get_dict_value(dataset, 'uri') or dataset.get('uri')
                if dataset_uri and dataset_uri in theme_labels_by_uri:
                    try:
                        labels = theme_labels_by_uri[dataset_uri]
                        if isinstance(labels, list) and labels:
                            dataset.setdefault('extras', []).append({
                                'key': 'bog_theme_labels',
                                'value': json.dumps(labels)
                            })
                    except Exception:
                        # If anything goes wrong, just skip attaching labels for this dataset
                        pass
                all_datasets.append(dataset)

            next_page_url = parser.next_page()

        log.info(f'[BoG] Collected {len(all_datasets)} raw datasets from feed')

        # Get source dataset for owner_org and name prefix
        source_dataset = model.Package.get(harvest_job.source.id)

        # Merge multilingual entries
        merged_datasets = self._merge_multilingual_datasets(all_datasets)
        log.info(f'[BoG] After merging: {len(merged_datasets)} datasets')

        # Create HarvestObjects for merged datasets
        try:
            for dataset in merged_datasets:
                # Generate name if missing
                if not dataset.get('name'):
                    title = self._get_best_title_for_name(dataset)
                    raw_name = self._gen_new_name(title)

                    if not raw_name:
                        log.warning(f"[BoG] Empty name from _gen_new_name for title={title!r}")
                        raw_name = 'dataset'

                    harvest_prefix = source_dataset.name if source_dataset and source_dataset.name else 'bank-athens'
                    dataset['name'] = f'{harvest_prefix}-{raw_name}'

                # Truncate FIRST to 95 chars (leaving room for suffix like "-1", "-2", etc.)
                if len(dataset['name']) > 95:
                    dataset['name'] = dataset['name'][:95]

                # Handle duplicate names (check both in-memory list AND existing packages in DB)
                base_name = dataset['name']
                suffix = 0

                while True:
                    candidate_name = base_name if suffix == 0 else f"{base_name}-{suffix}"

                    # Check if name is taken in this batch
                    if candidate_name in self._names_taken:
                        suffix += 1
                        continue

                    # Check if name exists in database
                    existing = model.Package.get(candidate_name)
                    if existing and existing.state == 'active':
                        # Check if it's a different dataset (different GUID)
                        current_guid = self._get_dict_value(dataset, 'uri') or self._get_dict_value(dataset, 'identifier')
                        existing_harvest_obj = model.Session.query(HarvestObject).filter(
                            HarvestObject.package_id == existing.id,
                            HarvestObject.current == True
                        ).first()

                        if existing_harvest_obj and existing_harvest_obj.guid != current_guid:
                            # Different dataset, need a new name
                            suffix += 1
                            continue

                    # Name is available
                    dataset['name'] = candidate_name
                    break

                if suffix > 0:
                    log.info(f"[BoG] Renamed duplicate '{base_name}' to '{dataset['name']}'")

                self._names_taken.append(dataset['name'])

                # Final length check (should already be <= 100 but just in case)
                if len(dataset['name']) > 100:
                    dataset['name'] = dataset['name'][:100]

                # Set owner_org from source
                if not dataset.get('owner_org') and source_dataset and source_dataset.owner_org:
                    dataset['owner_org'] = source_dataset.owner_org

                # Get GUID
                guid = self._get_guid(dataset, source_url=source_dataset.url if source_dataset else None)

                if not guid:
                    self._save_gather_error(f'[BoG] Could not get a unique identifier for dataset: {dataset.get("name", "unknown")}', harvest_job)
                    continue

                dataset.setdefault('extras', []).append({'key': 'guid', 'value': guid})
                guids_in_source.append(guid)

                obj = HarvestObject(guid=guid, job=harvest_job, content=json.dumps(dataset))
                obj.save()
                object_ids.append(obj.id)

                log.debug(f"[BoG] Created HarvestObject for: {dataset['name']} (guid: {guid})")

        except Exception as e:
            import traceback
            self._save_gather_error(f'[BoG] Error when processing datasets: {e} / {traceback.format_exc()}', harvest_job)
            return []

        # Mark datasets for deletion (not in source anymore)
        object_ids_to_delete = self._mark_datasets_for_deletion(guids_in_source, harvest_job)
        object_ids.extend(object_ids_to_delete)

        log.info(f'[BoG] Gather stage complete: {len(object_ids)} objects ({len(object_ids) - len(object_ids_to_delete)} new/update, {len(object_ids_to_delete)} to delete)')

        return object_ids

    def _get_best_title_for_name(self, dataset):
        """
        Get the best title to use for generating the dataset name/slug.

        Priority:
        1. English title (translates better to ASCII slug)
        2. Greek title
        3. Identifier or URI
        4. Fallback to 'Untitled Dataset'
        """
        # Try English from title_translated
        if dataset.get('title_translated'):
            en_title = dataset['title_translated'].get('en')
            if en_title and en_title.strip():
                return en_title.strip()

        # Try main title if it looks English (ASCII-ish)
        main_title = dataset.get('title', '')
        if main_title:
            # Check if mostly ASCII (likely English)
            ascii_ratio = sum(1 for c in main_title if ord(c) < 128) / max(len(main_title), 1)
            if ascii_ratio > 0.8:
                return main_title.strip()

        # Try Greek from title_translated
        if dataset.get('title_translated'):
            el_title = dataset['title_translated'].get('el')
            if el_title and el_title.strip():
                return el_title.strip()

        # Fallback to main title even if Greek
        if main_title:
            return main_title.strip()

        # Last resort
        return dataset.get('identifier') or dataset.get('uri') or 'Untitled Dataset'

    def _extract_theme_labels(self, themes):
        """
        Extract human-readable labels from theme objects.

        BoG themes come as complex objects with skos:prefLabel:
        <dcat:theme>
          <rdf:Description rdf:about="...">
            <skos:prefLabel xml:lang="en">Exchange Rates and Gold</skos:prefLabel>
          </rdf:Description>
        </dcat:theme>

        This method handles:
        - Strings (URIs or plain labels)
        - Dicts with prefLabel, label, name, or title keys
        - Lists of the above

        Returns a list of extracted label strings.
        """
        if not themes:
            return []

        if isinstance(themes, str):
            themes = [themes]

        labels = []

        for theme in themes:
            label_candidates = []

            if isinstance(theme, dict):
                # Try various keys that might contain the label
                # Priority: prefLabel > label > name > title > uri extraction
                raw_label = (
                    theme.get('prefLabel') or
                    theme.get('skos:prefLabel') or
                    theme.get('label') or
                    theme.get('name') or
                    theme.get('title')
                )

                # Handle multilingual labels (dict with lang codes)
                if isinstance(raw_label, dict):
                    # Collect both Greek and English where available, then any others
                    for lang_key in ('el', 'en'):
                        val = raw_label.get(lang_key)
                        if isinstance(val, str) and val.strip():
                            label_candidates.append(val)
                    if not label_candidates:
                        for val in raw_label.values():
                            if isinstance(val, str) and val.strip():
                                label_candidates.append(val)
                elif isinstance(raw_label, str):
                    label_candidates.append(raw_label)

                # If still no label, try to extract from URI
                if not label_candidates:
                    uri = theme.get('uri') or theme.get('@id') or theme.get('about')
                    if uri and isinstance(uri, str):
                        # Skip noisy URIs with query strings (eg BoG items?query=...)
                        if '?' in uri:
                            continue
                        # Extract last segment from URI
                        label = uri.rstrip('/').split('/')[-1]
                        # URL decode if needed
                        try:
                            from urllib.parse import unquote
                            label = unquote(label)
                        except Exception:
                            pass
                        if label:
                            label_candidates.append(label)

            elif isinstance(theme, str):
                theme_str = theme.strip()
                if 'http' in theme_str:
                    # Skip noisy URIs with query strings
                    if '?' in theme_str:
                        continue
                    # It's a URI, extract last segment
                    label = theme_str.rstrip('/').split('/')[-1]
                    try:
                        from urllib.parse import unquote
                        label = unquote(label)
                    except Exception:
                        pass
                else:
                    label = theme_str

                if label:
                    label_candidates.append(label)

            for candidate in label_candidates:
                if not isinstance(candidate, str):
                    continue
                clean = candidate.strip()
                if clean and clean not in labels:
                    labels.append(clean)
                    log.debug(f"[BoG] Extracted theme label: '{clean}'")

        return labels

    def modify_package_dict(self, package_dict, temp_dict, harvest_object):
        """
        Apply BoG-specific modifications to the package dict.
        """
        try:
            log.info(f"[BoG HARVESTER] Processing dataset: {package_dict.get('name', 'unknown')}")

            # 0. Ensure dataset name length <= 100 chars
            name = package_dict.get('name')
            if isinstance(name, str) and len(name) > 100:
                package_dict['name'] = name[:100]

            # 1. Extract theme labels and add as tags
            original_themes = package_dict.get('theme', [])
            theme_labels = self._extract_theme_labels(original_themes)

            # Also add BoG-specific theme labels extracted from the RDF graph
            bog_theme_labels = []
            for extra in package_dict.get('extras', []) or []:
                if not isinstance(extra, dict):
                    continue
                if extra.get('key') != 'bog_theme_labels':
                    continue
                raw_labels = extra.get('value')
                labels = []
                if isinstance(raw_labels, str):
                    try:
                        labels = json.loads(raw_labels)
                    except Exception:
                        labels = []
                elif isinstance(raw_labels, list):
                    labels = raw_labels
                for lbl in labels:
                    if isinstance(lbl, str):
                        clean = lbl.strip()
                        if clean:
                            bog_theme_labels.append(clean)
                break

            if bog_theme_labels:
                theme_labels.extend(bog_theme_labels)

            if theme_labels:
                current_tags = package_dict.get('tags', []) or []
                existing_tag_names = {
                    t['name'].strip().lower()
                    for t in current_tags
                    if isinstance(t, dict) and t.get('name')
                }

                for label in theme_labels:
                    if not isinstance(label, str):
                        continue
                    clean = label.strip()
                    if not clean:
                        continue
                    lower = clean.lower()
                    if lower not in existing_tag_names:
                        package_dict.setdefault('tags', []).append({'name': clean})
                        existing_tag_names.add(lower)
                        log.info(f"[BoG] Added theme as tag: '{clean}'")

            # 2. Hardcoded Theme Mapping (ECON)
            econ_uri = 'http://publications.europa.eu/resource/authority/data-theme/ECON'
            package_dict['theme'] = [econ_uri]

            # 3. Frequency Mapping
            if 'frequency' in package_dict:
                raw_freq = str(package_dict['frequency']).strip()
                mapped_freq = self._map_bog_frequency(raw_freq)
                if mapped_freq:
                    package_dict['frequency'] = mapped_freq

            # 4. License Mapping (dataset-level, pre-parent)
            self._map_bog_license(package_dict)

            # 5. Ensure multilingual fields are properly set
            self._ensure_multilingual_fields(package_dict)

            # 6. Infer resource languages from metadata / filenames
            self._infer_resource_languages(package_dict)

        except Exception as e:
            log.error(f"[BoG HARVESTER] Error in specific mapping (pre-parent): {e}", exc_info=True)

        # Do not persist helper extras like bog_theme_labels on the final dataset
        if package_dict.get('extras'):
            try:
                package_dict['extras'] = [
                    extra for extra in package_dict['extras']
                    if not (isinstance(extra, dict) and extra.get('key') == 'bog_theme_labels')
                ]
            except Exception:
                log.warning("[BoG] Failed to clean up bog_theme_labels extra", exc_info=True)
                # In case of any unexpected structure, leave extras as-is
                pass

        # Call Parent Logic for standard validation fixes
        package_dict = super().modify_package_dict(package_dict, temp_dict, harvest_object)

        # Post-parent: re-apply BoG license normalization and ensure all license fields are strings
        try:
            self._normalize_all_license_fields(package_dict)  # ALWAYS normalize to prevent Solr errors
            self._map_bog_license(package_dict)
        except Exception as e:
            log.error(f"[BoG HARVESTER] Error in specific mapping (post-parent): {e}", exc_info=True)

        return package_dict

    def _infer_resource_languages(self, package_dict):
        """
        Infer resource-level languages and populate language_options using:
        1) Existing resource language metadata (if present)
        2) Filename / URL patterns as a fallback (e.g. *_el_*, *_en_*)
        """
        if not isinstance(package_dict, dict):
            return

        resources = package_dict.get('resources') or []
        if not isinstance(resources, list) or not resources:
            return

        # Load controlled vocabulary values for Languages so that we store
        # values compatible with scheming (URIs or canonical names).
        try:
            language_uri_map = self._get_vocabulary_uri_map('Languages')
        except Exception:
            language_uri_map = {}

        if not language_uri_map:
            log.debug("[BoG] 'Languages' vocabulary not available; skipping resource and dataset language_options mapping")
            return

        def _code_from_language_uri(value):
            if not value or not isinstance(value, str):
                return None
            v = value.strip()
            if not v:
                return None

            lower = v.lower()
            # Publications Office URIs: .../language/ELL
            if 'publications.europa.eu/resource/authority/language/' in lower:
                return v.rstrip('/').split('/')[-1].upper()

            # ISO 639-1 URIs: .../iso639-1/el or .../iso639-1/en
            if 'id.loc.gov/vocabulary/iso639-1/' in lower:
                iso = v.rstrip('/').split('/')[-1].lower()
                if iso in ('el', 'ell', 'gr'):
                    return 'ELL'
                if iso in ('en', 'eng'):
                    return 'ENG'
                return iso.upper()

            return None

        def _code_from_text(text):
            if not text or not isinstance(text, str):
                return None
            t = text.lower()

            # Heuristics based on BoG file / URL naming conventions
            if '_el_' in t or '/el/' in t or t.endswith('_el.xls') or t.endswith('_el.xlsx') or t.endswith('_el.csv'):
                return 'ELL'
            if '_en_' in t or '/en/' in t or t.endswith('_en.xls') or t.endswith('_en.xlsx') or t.endswith('_en.csv'):
                return 'ENG'

            return None

        # Track all language codes detected at resource level so we can
        # also populate dataset-level language_options consistently.
        dataset_codes = set()

        for res in resources:
            if not isinstance(res, dict):
                continue

            # Do not override explicit language options
            if res.get('language_options'):
                # Still try to reflect these in dataset-level languages
                raw_vals = res.get('language_options')
                if isinstance(raw_vals, list):
                    for v in raw_vals:
                        code = _code_from_language_uri(v)
                        if code:
                            dataset_codes.add(code)
                continue

            codes = []

            # 1) Try to derive from resource['language'] (if present from DCAT)
            lang_field = res.get('language')
            if isinstance(lang_field, str) and lang_field.strip():
                raw_values = []
                try:
                    decoded = json.loads(lang_field)
                    if isinstance(decoded, list):
                        raw_values.extend(v for v in decoded if isinstance(v, str))
                    elif isinstance(decoded, str):
                        raw_values.append(decoded)
                except Exception:
                    raw_values.append(lang_field)

                for v in raw_values:
                    code = _code_from_language_uri(v)
                    if code and code not in codes:
                        codes.append(code)

            # 2) Fallback: infer from URL / filename patterns
            if not codes:
                text_parts = []
                for key in ('url', 'download_url', 'access_url', 'name'):
                    val = res.get(key)
                    if isinstance(val, str):
                        text_parts.append(val)
                text = ' '.join(text_parts)
                code = _code_from_text(text)
                if code and code not in codes:
                    codes.append(code)

            if not codes:
                continue

            # Map codes to vocabulary values expected by scheming
            values = []
            for code in codes:
                val = language_uri_map.get(code)
                if val and val not in values:
                    values.append(val)
                    dataset_codes.add(code)

            if values:
                res['language_options'] = values
                log.info(f"[BoG] Inferred resource language_options={values} for resource url={res.get('url')}")

        # Populate dataset-level language_options as the union of all
        # resource languages, merged with any existing dataset value.
        if dataset_codes:
            existing = package_dict.get('language_options')
            dataset_values = []
            if isinstance(existing, list):
                dataset_values.extend(
                    v for v in existing
                    if isinstance(v, str)
                )

            for code in dataset_codes:
                val = language_uri_map.get(code)
                if val and val not in dataset_values:
                    dataset_values.append(val)

            if dataset_values:
                package_dict['language_options'] = dataset_values
                log.info(f"[BoG] Inferred dataset language_options={dataset_values}")

    def _ensure_multilingual_fields(self, package_dict):
        """
        Ensure title_translated and notes_translated have both el and en.
        """
        # Handle title_translated
        title_trans = package_dict.get('title_translated', {})
        if not isinstance(title_trans, dict):
            title_trans = {}

        main_title = package_dict.get('title', '')

        if not title_trans.get('el'):
            title_trans['el'] = title_trans.get('en') or main_title or 'Χωρίς τίτλο'
        if not title_trans.get('en'):
            title_trans['en'] = title_trans.get('el') or main_title or 'Untitled'

        package_dict['title_translated'] = title_trans

        # Handle notes_translated
        notes_trans = package_dict.get('notes_translated', {})
        if not isinstance(notes_trans, dict):
            notes_trans = {}

        main_notes = package_dict.get('notes', '')

        if not notes_trans.get('el'):
            notes_trans['el'] = notes_trans.get('en') or main_notes or 'Περιγραφή συνόλου δεδομένων'
        if not notes_trans.get('en'):
            notes_trans['en'] = notes_trans.get('el') or main_notes or 'Dataset description'

        package_dict['notes_translated'] = notes_trans

    def _map_bog_frequency(self, frequency_text):
        """Map BoG frequency values to EU authority URIs."""
        if not frequency_text:
            return None
        raw = str(frequency_text).strip()
        text = raw.lower()
        base_uri = "http://publications.europa.eu/resource/authority/frequency/"

        mapping = {
            'annual': 'ANNUAL', 'annually': 'ANNUAL',
            'semi-annual': 'ANNUAL_2', 'semiannual': 'ANNUAL_2', 'half-yearly': 'ANNUAL_2',
            'quarterly': 'QUARTERLY', 'monthly': 'MONTHLY', 'weekly': 'WEEKLY',
            'daily': 'DAILY', 'irregular': 'IRREG', 'not planned': 'NOT_PLANNED',
            'never': 'NEVER', 'unknown': 'UNKNOWN',
            # Greek
            'ετήσια': 'ANNUAL', 'ετησια': 'ANNUAL',
            'εξαμηνιαία': 'ANNUAL_2', 'εξαμηνιαια': 'ANNUAL_2',
            'τριμηνιαία': 'QUARTERLY', 'τριμηνιαια': 'QUARTERLY',
            'μηνιαία': 'MONTHLY', 'μηνιαια': 'MONTHLY',
            'εβδομαδιαία': 'WEEKLY',
            'ημερήσια': 'DAILY', 'ημερησια': 'DAILY'
        }
        # Handle CLD frequency URIs like http://purl.org/cld/freq/Monthly or Semiannual
        if 'purl.org/cld/freq/' in text:
            cld_code = raw.rstrip('/').split('/')[-1].lower()
            if cld_code in mapping:
                return f"{base_uri}{mapping[cld_code]}"

        if text in mapping:
            return f"{base_uri}{mapping[text]}"

        # Fuzzy checks
        if 'έτος' in text or 'ετήσι' in text or 'year' in text:
            return f"{base_uri}ANNUAL"
        if 'εξάμην' in text:
            return f"{base_uri}ANNUAL_2"
        if 'τρίμην' in text or 'quarter' in text:
            return f"{base_uri}QUARTERLY"
        if 'μήνα' in text or 'μηνιαί' in text or 'month' in text:
            return f"{base_uri}MONTHLY"
        if 'ημέρα' in text or 'ημερήσ' in text or 'day' in text or 'daily' in text:
            return f"{base_uri}DAILY"
        if 'week' in text:
            return f"{base_uri}WEEKLY"

        return None

    def _normalize_all_license_fields(self, package_dict):
        """
        Normalize license fields to ensure they are strings, not lists.
        This is for database storage - Solr filtering is handled separately via before_index.
        """
        def _normalize_license_value(val):
            """Ensure license is a single string, not a list."""
            if isinstance(val, list):
                for v in val:
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                return None
            if isinstance(val, str):
                return val.strip() if val.strip() else None
            return None

        # Normalize dataset-level license fields
        for license_field in ['license', 'license_url', 'license_title', 'license_id']:
            if license_field in package_dict:
                normalized = _normalize_license_value(package_dict[license_field])
                if normalized:
                    package_dict[license_field] = normalized
                else:
                    del package_dict[license_field]

        # Normalize resource-level license fields (keep in DB, just ensure they're strings)
        resources = package_dict.get('resources') or []
        if isinstance(resources, list):
            for res in resources:
                if not isinstance(res, dict):
                    continue
                for license_field in ['license', 'license_url', 'license_title', 'license_id']:
                    if license_field in res:
                        normalized = _normalize_license_value(res[license_field])
                        if normalized:
                            res[license_field] = normalized
                        else:
                            res.pop(license_field, None)

    def _map_bog_license(self, package_dict):
        """
        Normalize dataset- and resource-level CC BY 4.0 licenses to the EU authority URI.
        """
        valid_uri = 'http://publications.europa.eu/resource/authority/licence/CC_BY_4_0'

        def _is_cc_by_40(val) -> bool:
            if isinstance(val, list):
                return any(_is_cc_by_40(v) for v in val)
            if not isinstance(val, str):
                return False
            v = val.lower()
            return (
                'creativecommons.org/licenses/by/4.0' in v
                or 'cc by 4.0' in v
                or 'attribution 4.0' in v
                or v.endswith('cc_by_4_0'.lower())
            )

        # 1) Check dataset-level hints
        candidates = [package_dict.get(k) for k in ['license_id', 'license_url', 'license_title', 'license']]
        found_cc = any(_is_cc_by_40(v) for v in candidates if v)

        # 2) Check resource-level licenses
        resources = package_dict.get('resources') or []
        cc_resources = []
        if isinstance(resources, list):
            for res in resources:
                if not isinstance(res, dict):
                    continue
                res_license = res.get('license') or res.get('license_url') or res.get('license_title')
                if res_license and _is_cc_by_40(res_license):
                    cc_resources.append(res)

        if not found_cc and not cc_resources:
            return

        # 3) Apply normalized license at dataset level
        package_dict['license'] = valid_uri
        package_dict['license_id'] = valid_uri

        # 4) Normalize resource-level licenses where we detected CC BY 4.0
        for res in cc_resources:
            res['license'] = valid_uri
