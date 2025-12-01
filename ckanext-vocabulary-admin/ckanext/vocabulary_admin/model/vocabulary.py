# -*- coding: utf-8 -*-
"""
Model for vocabulary management.
"""
import logging

from sqlalchemy import Column, Table, ForeignKey, types
from sqlalchemy.orm import relationship, backref

from ckan.model import meta, domain_object
from ckan.model import types as _types
from ckan.model.vocabulary import Vocabulary as CkanVocabulary
from ckan.model.tag import Tag as CkanTag
from ckanext.vocabulary_admin.model.tag_metadata import get_tag_metadata as _get_tag_metadata

log = logging.getLogger(__name__)




def get_vocabularies():
    """
    Get all vocabularies.
    """
    return meta.Session.query(CkanVocabulary).all()

def get_vocabulary(id_or_name):
    """
    Get a vocabulary by its id or name.
    """
    return CkanVocabulary.get(id_or_name)

def get_tags(vocabulary_id_or_name):
    """
    Get all tags for a vocabulary.
    """
    vocabulary = get_vocabulary(vocabulary_id_or_name)
    if not vocabulary:
        return []

    # Convert to list so we can sort in Python based on our custom
    # metadata (order_index) without relying on DB-specific NULL handling.
    tags = list(vocabulary.tags)

    def _sort_key(tag):
        metadata = _get_tag_metadata(tag.id)
        order_index = metadata.get('order_index') if metadata else None
        name = getattr(tag, 'display_name', None) or getattr(tag, 'name', None) or ''

        if order_index is not None:
            return (0, order_index, name.lower())
        return (1, name.lower())

    tags.sort(key=_sort_key)
    return tags

def get_vocabulary_count():
    """
    Get the number of vocabularies.
    """
    return len(get_vocabularies())

def get_tag_count(vocabulary_id_or_name=None):
    """
    Get the number of tags.
    If vocabulary_id_or_name is provided, only count tags in that vocabulary.
    """
    if vocabulary_id_or_name:
        tags = get_tags(vocabulary_id_or_name)
        return tags.count() if hasattr(tags, 'count') else len(tags)

    # Count all tags in all vocabularies
    count = 0
    for vocabulary in get_vocabularies():
        count += vocabulary.tags.count()
    return count
