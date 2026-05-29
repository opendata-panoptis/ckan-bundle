# -*- coding: utf-8 -*-
"""
Rules and helpers for vocabulary-specific admin behavior.
"""
import ckan.model as model
from sqlalchemy import func

PROTECTED_VOCABULARY_NAMES = (
    'Access right',
    'File Type - Non Proprietary Format',
    'Machine Readable File Format',
)
_PROTECTED_VOCABULARY_NAMES_NORMALIZED = frozenset(
    str(name).strip().lower() for name in PROTECTED_VOCABULARY_NAMES
)


def normalize_text(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return text.lower()


def is_protected_vocabulary(vocabulary_name):
    normalized_name = normalize_text(vocabulary_name)
    if not normalized_name:
        return False

    return normalized_name in _PROTECTED_VOCABULARY_NAMES_NORMALIZED


def get_vocabulary_by_id_or_name(value):
    if not value:
        return None

    vocabulary = model.Vocabulary.get(value)
    if vocabulary:
        return vocabulary

    normalized_value = normalize_text(value)
    if not normalized_value:
        return None

    return (
        model.Session.query(model.Vocabulary)
        .filter(func.lower(model.Vocabulary.name) == normalized_value)
        .first()
    )


def parse_is_active_values(values, default=True):
    if not values:
        return default

    raw_value = values[-1]
    normalized_value = normalize_text(raw_value)

    if normalized_value in {'true', '1', 'yes', 'on'}:
        return True

    if normalized_value in {'false', '0', 'no', 'off', ''}:
        return False

    return default
