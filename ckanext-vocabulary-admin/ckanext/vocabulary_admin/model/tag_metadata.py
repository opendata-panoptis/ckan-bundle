# -*- coding: utf-8 -*-
"""
Model for tag metadata.
"""
import logging
import uuid

from sqlalchemy import Column, Table, ForeignKey, types, Boolean
from sqlalchemy.orm import relationship

from ckan.model import meta, domain_object
from ckan.model import types as _types
from ckan.model.tag import Tag as CkanTag

log = logging.getLogger(__name__)

vocabulary_tag_metadata_table = Table('vocabulary_tag_metadata', meta.metadata,
    Column('id', types.UnicodeText, primary_key=True, default=lambda: str(uuid.uuid4())),
    Column('tag_id', types.UnicodeText, ForeignKey('tag.id'), nullable=False, unique=True),
    Column('value_uri', types.UnicodeText),
    Column('label_el', types.UnicodeText),
    Column('label_en', types.UnicodeText),
    Column('description_el', types.UnicodeText),
    Column('description_en', types.UnicodeText),
    Column('is_active', Boolean, default=True, nullable=False),
)

class VocabularyTagMetadata(domain_object.DomainObject):
    """
    A model for storing additional metadata for vocabulary tags.
    This extends the functionality of CKAN's built-in Tag model.
    """
    # Properties will be added by the mapper

    @classmethod
    def get(cls, tag_id=None, **kwargs):
        """
        Get a tag metadata object by tag_id or other attributes.
        """
        query = meta.Session.query(cls)

        if tag_id:
            return query.filter(cls.tag_id == tag_id).first()

        return query.filter_by(**kwargs).first()

    @classmethod
    def create(cls, tag_id, value_uri=None, label_el=None, label_en=None, 
               description_el=None, description_en=None, is_active=True):
        """
        Create a new tag metadata object.
        """
        metadata = cls(
            tag_id=tag_id,
            value_uri=value_uri,
            label_el=label_el,
            label_en=label_en,
            description_el=description_el,
            description_en=description_en,
            is_active=is_active
        )

        meta.Session.add(metadata)
        meta.Session.commit()

        return metadata

    @classmethod
    def update(cls, tag_id, value_uri=None, label_el=None, label_en=None, 
               description_el=None, description_en=None, is_active=None):
        """
        Update an existing tag metadata object.
        """
        metadata = cls.get(tag_id=tag_id)

        if not metadata:
            return cls.create(
                tag_id=tag_id,
                value_uri=value_uri,
                label_el=label_el,
                label_en=label_en,
                description_el=description_el,
                description_en=description_en,
                is_active=True if is_active is None else is_active
            )

        if value_uri is not None:
            metadata.value_uri = value_uri
        if label_el is not None:
            metadata.label_el = label_el
        if label_en is not None:
            metadata.label_en = label_en
        if description_el is not None:
            metadata.description_el = description_el
        if description_en is not None:
            metadata.description_en = description_en
        if is_active is not None:
            metadata.is_active = is_active

        meta.Session.add(metadata)
        meta.Session.commit()

        return metadata

def get_tag_metadata(tag_id):
    """
    Get metadata for a tag.

    Args:
        tag_id: The ID of the tag.

    Returns:
        A dictionary containing the tag metadata, or None if no metadata exists.
    """
    metadata = VocabularyTagMetadata.get(tag_id=tag_id)
    if not metadata:
        return None

    return {
        'value_uri': metadata.value_uri,
        'label_el': metadata.label_el,
        'label_en': metadata.label_en,
        'description_el': metadata.description_el,
        'description_en': metadata.description_en,
        'is_active': metadata.is_active
    }


# Map the class to the table
meta.registry.map_imperatively(VocabularyTagMetadata, vocabulary_tag_metadata_table, properties={
    'tag': relationship(CkanTag, backref='metadata')
})
