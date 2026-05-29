# -*- coding: utf-8 -*-
"""
Guard checks for tag deactivation in vocabulary admin.
"""
import json
import logging
import re
from collections import defaultdict

import ckan.model as model

from ckanext.vocabulary_admin.model.tag_metadata import VocabularyTagMetadata
from ckanext.vocabulary_admin.vocabulary_rules import (
    get_vocabulary_by_id_or_name,
    normalize_text,
)

log = logging.getLogger(__name__)


try:
    from ckanext.scheming import helpers as scheming_helpers
except ImportError:  # pragma: no cover - optional dependency
    scheming_helpers = None


_URI_PARTS_SPLIT = re.compile(r'[/#]+')


def _empty_result():
    return {
        'blocked': False,
        'has_organization_targets': False,
        'counts': {
            'datasets': {'total': 0, 'total_with_resource_usage': 0, 'fields': {}},
            'resources': {'total': 0, 'fields': {}},
            'organizations': {'total': 0, 'fields': {}},
        },
        'total': 0,
    }


def _normalize_match_value(value):
    normalized = normalize_text(value)
    if not normalized:
        return None
    return normalized.rstrip('/')


def _extract_code_from_uri(value_uri):
    if not value_uri:
        return None

    stripped = str(value_uri).strip().rstrip('/')
    if not stripped:
        return None

    parts = [part for part in _URI_PARTS_SPLIT.split(stripped) if part]
    if not parts:
        return None

    return parts[-1]


def _build_tag_candidate_values(tag):
    metadata = VocabularyTagMetadata.get(tag_id=tag.id)
    value_uri = metadata.value_uri if metadata else None
    uri_code = _extract_code_from_uri(value_uri)

    candidates = set()
    for value in (tag.name, value_uri, uri_code):
        normalized = _normalize_match_value(value)
        if normalized:
            candidates.add(normalized)

    return candidates


def _looks_like_json(value):
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return bool(stripped) and stripped[0] in {'[', '{'}


def _decode_json_if_possible(value):
    if not _looks_like_json(value):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def _iter_atomic_strings(value):
    parsed = _decode_json_if_possible(value)
    if isinstance(parsed, str):
        stripped = parsed.strip()
        if stripped:
            yield stripped
        return

    if isinstance(parsed, dict):
        for child in parsed.values():
            for item in _iter_atomic_strings(child):
                yield item
        return

    if isinstance(parsed, (list, tuple, set)):
        for child in parsed:
            for item in _iter_atomic_strings(child):
                yield item


def _iter_subfield_values(value, subfield_name):
    parsed = _decode_json_if_possible(value)

    if isinstance(parsed, dict):
        if subfield_name in parsed:
            yield parsed[subfield_name]
        for child in parsed.values():
            for item in _iter_subfield_values(child, subfield_name):
                yield item
        return

    if isinstance(parsed, (list, tuple, set)):
        for child in parsed:
            for item in _iter_subfield_values(child, subfield_name):
                yield item
        return

    nested = _decode_json_if_possible(parsed)
    if nested is not parsed:
        for item in _iter_subfield_values(nested, subfield_name):
            yield item


def _value_matches_candidates(value, candidates, subfield_name=None):
    if not candidates:
        return False

    if subfield_name:
        values_to_check = []
        for nested in _iter_subfield_values(value, subfield_name):
            values_to_check.extend(_iter_atomic_strings(nested))
    else:
        values_to_check = list(_iter_atomic_strings(value))

    for item in values_to_check:
        normalized = _normalize_match_value(item)
        if normalized and normalized in candidates:
            return True

    return False


def _reference_points_to_vocabulary(vocabulary_reference, vocabulary):
    if not vocabulary_reference or not vocabulary:
        return False

    normalized_reference = _normalize_match_value(vocabulary_reference)
    if not normalized_reference:
        return False

    normalized_vocabulary_id = _normalize_match_value(vocabulary.id)
    normalized_vocabulary_name = _normalize_match_value(vocabulary.name)
    if normalized_reference in {normalized_vocabulary_id, normalized_vocabulary_name}:
        return True

    resolved = get_vocabulary_by_id_or_name(vocabulary_reference)
    return bool(resolved and resolved.id == vocabulary.id)


def _collect_targets_from_fields(field_list, entity_name, schema_type, vocabulary, targets):
    for field in field_list or []:
        field_name = field.get('field_name')
        if not field_name:
            continue

        kwargs = field.get('form_choices_helper_kwargs', {})
        if _reference_points_to_vocabulary(kwargs.get('vocabulary_id_or_name'), vocabulary):
            targets[(entity_name, field_name, None)].add(schema_type)

        for subfield in field.get('repeating_subfields') or []:
            subfield_name = subfield.get('field_name')
            if not subfield_name:
                continue
            subfield_kwargs = subfield.get('form_choices_helper_kwargs', {})
            if _reference_points_to_vocabulary(
                subfield_kwargs.get('vocabulary_id_or_name'),
                vocabulary,
            ):
                targets[(entity_name, field_name, subfield_name)].add(schema_type)


def _build_usage_targets(vocabulary):
    targets = defaultdict(set)
    if not scheming_helpers:
        return targets

    dataset_schemas = scheming_helpers.scheming_dataset_schemas(expanded=True) or {}
    organization_schemas = scheming_helpers.scheming_organization_schemas(expanded=True) or {}

    for dataset_type, schema in dataset_schemas.items():
        _collect_targets_from_fields(
            schema.get('dataset_fields'),
            'dataset',
            dataset_type,
            vocabulary,
            targets,
        )
        _collect_targets_from_fields(
            schema.get('resource_fields'),
            'resource',
            dataset_type,
            vocabulary,
            targets,
        )

    for organization_type, schema in organization_schemas.items():
        _collect_targets_from_fields(
            schema.get('fields'),
            'organization',
            organization_type,
            vocabulary,
            targets,
        )

    return targets


def _count_dataset_matches(field_name, schema_types, candidates, subfield_name=None):
    dataset_ids = set()
    dataset_types = list(schema_types)
    package_columns = {column.name for column in model.Package.__table__.columns}

    if field_name in package_columns:
        query = model.Session.query(model.Package.id, getattr(model.Package, field_name))
        query = query.filter(model.Package.state != 'deleted')
        if dataset_types:
            query = query.filter(model.Package.type.in_(dataset_types))
        for package_id, value in query:
            if _value_matches_candidates(value, candidates, subfield_name=subfield_name):
                dataset_ids.add(package_id)
        return dataset_ids

    query = (
        model.Session.query(model.PackageExtra.package_id, model.PackageExtra.value)
        .join(model.Package, model.PackageExtra.package_id == model.Package.id)
        .filter(model.PackageExtra.key == field_name)
        .filter(model.PackageExtra.state != 'deleted')
        .filter(model.Package.state != 'deleted')
    )
    if dataset_types:
        query = query.filter(model.Package.type.in_(dataset_types))

    for package_id, value in query:
        if _value_matches_candidates(value, candidates, subfield_name=subfield_name):
            dataset_ids.add(package_id)

    return dataset_ids


def _count_resource_matches(field_name, schema_types, candidates, subfield_name=None):
    resource_ids = set()
    dataset_types = list(schema_types)
    resource_columns = {column.name for column in model.Resource.__table__.columns}

    if field_name in resource_columns:
        query = (
            model.Session.query(model.Resource.id, getattr(model.Resource, field_name))
            .join(model.Package, model.Resource.package_id == model.Package.id)
            .filter(model.Resource.state != 'deleted')
            .filter(model.Package.state != 'deleted')
        )
        if dataset_types:
            query = query.filter(model.Package.type.in_(dataset_types))
        for resource_id, value in query:
            if _value_matches_candidates(value, candidates, subfield_name=subfield_name):
                resource_ids.add(resource_id)
        return resource_ids

    query = (
        model.Session.query(model.Resource.id, model.Resource.extras)
        .join(model.Package, model.Resource.package_id == model.Package.id)
        .filter(model.Resource.state != 'deleted')
        .filter(model.Package.state != 'deleted')
        .filter(model.Resource.extras.isnot(None))
    )
    if dataset_types:
        query = query.filter(model.Package.type.in_(dataset_types))

    for resource_id, extras in query:
        if not extras or field_name not in extras:
            continue
        if _value_matches_candidates(
            extras.get(field_name),
            candidates,
            subfield_name=subfield_name,
        ):
            resource_ids.add(resource_id)

    return resource_ids


def _count_organization_matches(field_name, schema_types, candidates, subfield_name=None):
    organization_ids = set()
    organization_types = list(schema_types)
    group_columns = {column.name for column in model.Group.__table__.columns}

    if field_name in group_columns:
        query = model.Session.query(model.Group.id, getattr(model.Group, field_name))
        query = query.filter(model.Group.state != 'deleted')
        if organization_types:
            query = query.filter(model.Group.type.in_(organization_types))
        for group_id, value in query:
            if _value_matches_candidates(value, candidates, subfield_name=subfield_name):
                organization_ids.add(group_id)
        return organization_ids

    query = (
        model.Session.query(model.GroupExtra.group_id, model.GroupExtra.value)
        .join(model.Group, model.GroupExtra.group_id == model.Group.id)
        .filter(model.GroupExtra.key == field_name)
        .filter(model.GroupExtra.state != 'deleted')
        .filter(model.Group.state != 'deleted')
    )
    if organization_types:
        query = query.filter(model.Group.type.in_(organization_types))

    for group_id, value in query:
        if _value_matches_candidates(value, candidates, subfield_name=subfield_name):
            organization_ids.add(group_id)

    return organization_ids


def check_tag_deactivation_allowed(tag_id):
    result = _empty_result()
    tag = model.Tag.get(tag_id)
    if not tag:
        return result

    vocabulary = model.Vocabulary.get(tag.vocabulary_id)
    if not vocabulary:
        return result

    candidates = _build_tag_candidate_values(tag)
    if not candidates:
        return result

    targets = _build_usage_targets(vocabulary)
    if not targets:
        return result
    result['has_organization_targets'] = any(
        entity_name == 'organization' for (entity_name, _, _) in targets.keys()
    )

    entity_field_matches = {
        'datasets': defaultdict(set),
        'resources': defaultdict(set),
        'organizations': defaultdict(set),
    }
    entity_total_matches = {
        'datasets': set(),
        'resources': set(),
        'organizations': set(),
    }

    for (entity_name, field_name, subfield_name), schema_types in targets.items():
        if entity_name == 'dataset':
            matched_ids = _count_dataset_matches(
                field_name,
                schema_types,
                candidates,
                subfield_name=subfield_name,
            )
            key = 'datasets'
        elif entity_name == 'resource':
            matched_ids = _count_resource_matches(
                field_name,
                schema_types,
                candidates,
                subfield_name=subfield_name,
            )
            key = 'resources'
        else:
            matched_ids = _count_organization_matches(
                field_name,
                schema_types,
                candidates,
                subfield_name=subfield_name,
            )
            key = 'organizations'

        if not matched_ids:
            continue

        display_field_name = field_name
        if subfield_name:
            display_field_name = '{0}.{1}'.format(field_name, subfield_name)

        entity_field_matches[key][display_field_name].update(matched_ids)
        entity_total_matches[key].update(matched_ids)

    total = 0
    for key in ('datasets', 'resources', 'organizations'):
        field_counts = {
            field_name: len(ids)
            for field_name, ids in sorted(entity_field_matches[key].items())
        }
        entity_total = len(entity_total_matches[key])
        result['counts'][key] = {
            'total': entity_total,
            'fields': field_counts,
        }
        total += entity_total

    dataset_ids_with_resource_usage = set(entity_total_matches['datasets'])
    resource_ids = list(entity_total_matches['resources'])
    if resource_ids:
        resource_package_query = (
            model.Session.query(model.Resource.package_id)
            .join(model.Package, model.Resource.package_id == model.Package.id)
            .filter(model.Resource.id.in_(resource_ids))
            .filter(model.Resource.state != 'deleted')
            .filter(model.Package.state != 'deleted')
        )
        dataset_ids_with_resource_usage.update(
            package_id for (package_id,) in resource_package_query if package_id
        )
    result['counts']['datasets']['total_with_resource_usage'] = len(
        dataset_ids_with_resource_usage
    )

    result['total'] = total
    result['blocked'] = total > 0
    return result
