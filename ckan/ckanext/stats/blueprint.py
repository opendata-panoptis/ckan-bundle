# encoding: utf-8
from __future__ import annotations
import os
import re
from typing import Any

from flask import Blueprint, current_app
from jinja2 import meta

from ckan.plugins.toolkit import render, h
import ckanext.stats.stats as stats_lib


stats = Blueprint(u'stats', __name__)
STATS_INDEX_TEMPLATE = u'ckanext/stats/index.html'
CORE_STATS_TEMPLATE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), u'templates')
)
CKAN_EXTENDS_RE = re.compile(r'{%\s*ckan_extends\b')
FULL_DATA_TEMPLATE_MARKERS = (
    'largest_groups',
    'top_tags',
    'top_package_creators',
    'most_edited_packages',
    'new_packages_by_week',
    'deleted_packages_by_week',
    'num_packages_by_week',
    'package_revisions_by_week',
    'raw_packages_by_week',
    'raw_all_package_revisions',
    'raw_new_datasets',
    'raw_deleted_datasets',
)


def _stats_index_source_uses_full_data(
        source: str, filename: str | None) -> bool:
    if not filename:
        return True

    try:
        is_core_template = os.path.commonpath([
            os.path.abspath(filename),
            CORE_STATS_TEMPLATE_DIR,
        ]) == CORE_STATS_TEMPLATE_DIR
    except ValueError:
        return True

    if is_core_template:
        return True

    if CKAN_EXTENDS_RE.search(source):
        return True

    try:
        parsed = current_app.jinja_env.parse(source)
    except Exception:
        return True

    used_variables = meta.find_undeclared_variables(parsed)
    return bool(set(FULL_DATA_TEMPLATE_MARKERS) & used_variables)


def _stats_index_uses_full_data() -> bool:
    try:
        source, filename, _uptodate = current_app.jinja_env.loader.get_source(
            current_app.jinja_env,
            STATS_INDEX_TEMPLATE,
        )
    except Exception:
        return True

    return _stats_index_source_uses_full_data(source, filename)


def _stats_index_extra_vars(stats: stats_lib.Stats) -> dict[str, Any]:
    new_packages_by_week = stats.get_by_week('new_packages')
    deleted_packages_by_week = stats.get_by_week('deleted_packages')
    num_packages_by_week = stats.get_num_packages_by_week()
    package_revisions_by_week = stats.get_by_week('package_revisions')

    extra_vars: dict[str, Any] = {
        'largest_groups': stats.largest_groups(),
        'top_tags': stats.top_tags(),
        'top_package_creators': stats.top_package_creators(),
        'most_edited_packages': stats.most_edited_packages(),
        'new_packages_by_week': new_packages_by_week,
        'deleted_packages_by_week': deleted_packages_by_week,
        'num_packages_by_week': num_packages_by_week,
        'package_revisions_by_week': package_revisions_by_week
    }

    extra_vars['raw_packages_by_week'] = []
    for week_date, num_packages, cumulative_num_packages\
            in num_packages_by_week:
        extra_vars['raw_packages_by_week'].append(
            {'date': h.date_str_to_datetime(week_date),
             'total_packages': cumulative_num_packages})

    extra_vars['raw_all_package_revisions'] = []
    for week_date, _revs, num_revisions, _cumulative_num_revisions\
            in package_revisions_by_week:
        extra_vars['raw_all_package_revisions'].append(
            {'date': h.date_str_to_datetime(week_date),
             'total_revisions': num_revisions})

    extra_vars['raw_new_datasets'] = []
    for week_date, _pkgs, num_packages, _cumulative_num_revisions\
            in new_packages_by_week:
        extra_vars['raw_new_datasets'].append(
            {'date': h.date_str_to_datetime(week_date),
             'new_packages': num_packages})

    extra_vars['raw_deleted_datasets'] = []
    for week_date, _pkgs, num_packages, cumulative_num_packages\
            in deleted_packages_by_week:
        extra_vars['raw_deleted_datasets'].append(
            {'date': h.date_str_to_datetime(
                week_date), 'deleted_packages': num_packages})

    return extra_vars


@stats.route(u'/stats')
def index():
    extra_vars: dict[str, Any] = {}
    if _stats_index_uses_full_data():
        extra_vars = _stats_index_extra_vars(stats_lib.Stats())

    return render(STATS_INDEX_TEMPLATE, extra_vars)

@stats.route(u'/stats/datasets-by-theme')
def datasets_by_theme():
    stats = stats_lib.Stats()
    extra_vars: dict[str, Any] = {
        'datasets_by_theme': stats.datasets_by_theme()
    }
    return render(u'ckanext/stats/datasets_by_theme.html', extra_vars)

@stats.route(u'/stats/stats-organizations-per-type')
def organizations_by_publisher_type():
    stats = stats_lib.Stats()
    extra_vars: dict[str, Any] = {
        'organizations_by_publisher_type': stats.organizations_by_publisher_type(),
        'organizations_stats': stats.organization_publisher_type_summary(),
    }
    return render(u'ckanext/stats/organizations_by_publisher_type.html', extra_vars)
