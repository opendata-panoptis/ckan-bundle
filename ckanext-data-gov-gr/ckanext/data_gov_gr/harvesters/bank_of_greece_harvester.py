# -*- coding: utf-8 -*-
import logging
from .custom_dcat_harvester import CustomDcatHarvester

log = logging.getLogger(__name__)


class BankOfGreeceHarvester(CustomDcatHarvester):
    """
    Custom Harvester specifically for Bank of Greece.
    Inherits validation fixes from CustomDcatHarvester.
    """

    def info(self):
        return {
            'name': 'bank_of_greece_harvester',
            'title': 'Bank of Greece DCAT Harvester',
            'description': 'Harvester for BoG with forced ECON theme, custom frequency and license mapping.',
            'form_config_interface': 'Text',
            'show_config': False
        }

    def validate_config(self, source_config):
        """
        Ensure a sane default RDF format for BoG sources.

        BoG endpoints typically return RDF/XML but may use a generic
        \"text/xml\" content-type, which rdflib does not recognize as a
        registered RDF parser format. By forcing rdf_format=\"xml\" when
        unset, we avoid \"No plugin registered for (text/xml, ...)\" errors.
        """
        import json

        if not source_config:
            return json.dumps({'rdf_format': 'xml'})

        try:
            conf = json.loads(source_config) or {}
        except ValueError:
            # Let the parent raise a proper error
            return super().validate_config(source_config)

        if not conf.get('rdf_format'):
            conf['rdf_format'] = 'xml'

        # Delegate to parent for standard validation (max_pages, etc.)
        return super().validate_config(json.dumps(conf))

    def modify_package_dict(self, package_dict, temp_dict, harvest_object):
        try:
            log.info(f"[BoG HARVESTER] Processing dataset: {package_dict.get('name', 'unknown')}")

            # 0. Ensure dataset name length <= 100 chars
            name = package_dict.get('name')
            if isinstance(name, str) and len(name) > 100:
                package_dict['name'] = name[:100]

            # 1. Move Original Themes/Categories to Tags
            original_themes = package_dict.get('theme', [])
            if isinstance(original_themes, str):
                original_themes = [original_themes]

            if original_themes:
                current_tags = package_dict.get('tags', [])
                existing_tag_names = {t['name'].lower() for t in current_tags if 'name' in t}

                for theme_val in original_themes:
                    if not theme_val or not isinstance(theme_val, str):
                        continue
                    tag_label = theme_val.strip()
                    if 'http' in tag_label:
                        tag_label = tag_label.rsplit('/', 1)[-1]

                    if tag_label and tag_label.lower() not in existing_tag_names:
                        package_dict.setdefault('tags', []).append({'name': tag_label})
                        existing_tag_names.add(tag_label.lower())

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

        except Exception as e:
            log.error(f"[BoG HARVESTER] Error in specific mapping (pre-parent): {e}", exc_info=True)

        # Call Parent Logic for standard validation fixes
        package_dict = super().modify_package_dict(package_dict, temp_dict, harvest_object)

        # Post-parent: re-apply BoG license normalization, especially on
        # resource-level licenses that may have been preserved from source.
        try:
            self._map_bog_license(package_dict)
        except Exception as e:
            log.error(f"[BoG HARVESTER] Error in specific mapping (post-parent): {e}", exc_info=True)

        return package_dict

    def _map_bog_frequency(self, frequency_text):
        if not frequency_text:
            return None
        text = str(frequency_text).lower().strip()
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

    def _map_bog_license(self, package_dict):
        """
        Normalize dataset- and resource-level CC BY 4.0 licenses to the EU authority URI.

        BoG feeds use various CC BY 4.0 URLs (eg legalcode, legalcode.el). We:
          - Detect CC BY 4.0 hints on dataset or resources
          - Force dataset license/license_id to the EU URI
          - Force any resource-level CC BY 4.0 license to the same URI
        """
        valid_uri = 'http://publications.europa.eu/resource/authority/licence/CC_BY_4_0'

        def _is_cc_by_40(val: str) -> bool:
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
        found_cc = any(_is_cc_by_40(v) for v in candidates if isinstance(v, str))

        # 2) Scan resource-level licenses
        resources = package_dict.get('resources') or []
        cc_resources = []
        if isinstance(resources, list):
            for res in resources:
                if not isinstance(res, dict):
                    continue
                res_license = res.get('license') or res.get('license_url') or res.get('license_title')
                if isinstance(res_license, str) and _is_cc_by_40(res_license):
                    cc_resources.append(res)

        if not found_cc and not cc_resources:
            return

        # 3) Apply normalized license at dataset level
        package_dict['license'] = valid_uri
        package_dict['license_id'] = valid_uri

        # 4) Normalize resource-level licenses where we detected CC BY 4.0
        for res in cc_resources:
            res['license'] = valid_uri
