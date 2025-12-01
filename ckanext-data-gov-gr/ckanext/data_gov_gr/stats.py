from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Optional

from ckan.plugins import toolkit as tk

import ckanext.stats.stats as core_stats

log = logging.getLogger(__name__)


class DataGovStats(core_stats.Stats):

    def datasets_by_publisher_type(self) -> list[tuple[str, str, int]]:
        """Return number of datasets per publisher type with both code and label."""
        try:
            vocab_data = tk.get_action('vocabularyadmin_vocabulary_show')({}, {
                'id': 'Publisher type'
            })

            current_lang = tk.h.lang()
            label_field = f'label_{current_lang}'

            uri_to_code_label: dict[str, tuple[str, str]] = {}
            for tag in vocab_data.get('tags', []):
                value_uri = tag.get('value_uri')
                if not value_uri:
                    continue
                code = value_uri.split('/')[-1]
                label = tag.get(label_field) or code
                uri_to_code_label[value_uri] = (code, label)

            orgs = tk.get_action('organization_list')({}, {
                'all_fields': True,
                'include_extras': True
            })
            org_id_to_type: dict[str, tuple[str, str]] = {}
            for org in orgs:
                publisher_type_uri = org.get('publishertype')
                if publisher_type_uri and publisher_type_uri in uri_to_code_label:
                    org_id_to_type[org['id']] = uri_to_code_label[publisher_type_uri]

            if not org_id_to_type:
                return []

            search = tk.get_action('package_search')({}, {
                'q': '*:*',
                'fq': 'dataset_type:dataset',
                'rows': 0,
                'facet': 'on',
                'facet.field': ['owner_org'],
                'facet.limit': -1,
                'facet.mincount': 1
            })

            counts_by_type: dict[str, dict[str, int | str]] = {}
            items: Iterable[dict[str, Optional[str]]] = search.get('search_facets', {}).get('owner_org', {}).get('items', [])
            for it in items:
                org_id = it.get('name')
                if not org_id or org_id not in org_id_to_type:
                    continue
                count = int(it.get('count', 0))
                code, label = org_id_to_type[org_id]
                if code in counts_by_type:
                    counts_by_type[code]['count'] = int(counts_by_type[code]['count']) + count  # type: ignore
                else:
                    counts_by_type[code] = {'label': label, 'count': count}

            results = [
                (code, data['label'], int(data['count']))  # type: ignore[arg-type]
                for code, data in counts_by_type.items()
            ]
            return sorted(results, key=lambda x: -x[2])

        except tk.ObjectNotFound:
            return []
        except Exception as e:  # pragma: no cover - defensive
            log.error('Error in datasets_by_publisher_type: %s', e)
            return []

    def datasets_by_organization(self) -> list[tuple[str, str, int]]:
        """Return number of datasets per organization."""
        try:
            orgs = tk.get_action('organization_list')({}, {
                'all_fields': True,
                'include_extras': False
            })
            org_id_to_title: dict[str, str] = {
                org['id']: (org.get('title') or org.get('name'))
                for org in orgs
            }

            search = tk.get_action('package_search')({}, {
                'q': '*:*',
                'fq': 'dataset_type:dataset',
                'rows': 0,
                'facet': 'on',
                'facet.field': ['owner_org'],
                'facet.limit': -1,
                'facet.mincount': 1
            })

            items: Iterable[dict[str, Optional[str]]] = search.get('search_facets', {}).get('owner_org', {}).get('items', [])
            results: list[tuple[str, str, int]] = []
            for it in items:
                org_id = it.get('name')
                if not org_id:
                    continue
                count = int(it.get('count', 0))
                title = org_id_to_title.get(org_id, org_id)
                results.append((org_id, title, count))

            return sorted(results, key=lambda x: -x[2])

        except tk.ObjectNotFound:
            return []
        except Exception as e:  # pragma: no cover - defensive
            log.error('Error in datasets_by_organization: %s', e)
            return []

    def datasets_vs_services(self) -> dict[str, int]:
        """Return total counts for datasets and data services."""
        try:
            datasets_res = tk.get_action('package_search')({}, {
                'q': '*:*',
                'fq': 'dataset_type:dataset',
                'rows': 0
            })
            services_res = tk.get_action('package_search')({}, {
                'q': '*:*',
                'fq': 'dataset_type:data-service',
                'rows': 0
            })
            return {
                'datasets': int(datasets_res.get('count', 0)),
                'data_services': int(services_res.get('count', 0)),
            }
        except Exception as e:  # pragma: no cover - defensive
            log.error('Error in datasets_vs_services: %s', e)
            return {'datasets': 0, 'data_services': 0}

    def datasets_by_hvd_category(self) -> list[tuple[str, str, int]]:
        """Return number of datasets per High-Value Dataset category with code and label."""
        try:
            current_lang = tk.h.lang()
            label_field = f'label_{current_lang}'

            vocab_data = tk.get_action('vocabularyadmin_vocabulary_show')({}, {
                'id': 'High-value dataset categories'
            })

            results: list[tuple[str, str, int]] = []
            for tag in vocab_data.get('tags', []):
                value_uri = tag.get('value_uri')
                if not value_uri:
                    continue
                code = value_uri.split('/')[-1]
                label = tag.get(label_field) or code

                search_result = tk.get_action('package_search')({}, {
                    'q': f'hvd_category:*{code}*',
                    'fq': 'dataset_type:dataset',
                    'rows': 0
                })
                results.append((code, label, int(search_result.get('count', 0))))

            return sorted(results, key=lambda x: -x[2])
        except tk.ObjectNotFound:
            return []
        except Exception as e:  # pragma: no cover - defensive
            log.error('Error in datasets_by_hvd_category: %s', e)
            return []


# Expose data.gov.gr specific stats on the core Stats class so existing
# endpoints that depend on ``ckanext.stats`` can access them without
# touching core CKAN code.
for _method in (
    'datasets_by_publisher_type',
    'datasets_by_organization',
    'datasets_vs_services',
    'datasets_by_hvd_category',
):
    setattr(core_stats.Stats, _method, getattr(DataGovStats, _method))
