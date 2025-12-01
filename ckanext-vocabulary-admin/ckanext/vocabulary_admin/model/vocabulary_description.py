# -*- coding: utf-8 -*-
"""
Model for vocabulary descriptions.
"""
import logging
import uuid

from sqlalchemy import Column, Table, ForeignKey, types
from sqlalchemy.orm import relationship

from ckan.model import meta, domain_object
from ckan.model import types as _types
from ckan.model.vocabulary import Vocabulary as CkanVocabulary

log = logging.getLogger(__name__)

vocabulary_description_table = Table('vocabulary_description', meta.metadata,
    Column('id', types.UnicodeText, primary_key=True, default=lambda: str(uuid.uuid4())),
    Column('vocabulary_id', types.UnicodeText, ForeignKey('vocabulary.id'), nullable=False, unique=True),
    Column('description_el', types.UnicodeText),
    Column('description_en', types.UnicodeText),
)

class VocabularyDescription(domain_object.DomainObject):
    """
    A model for storing additional descriptions for vocabularies.
    This extends the functionality of CKAN's built-in Vocabulary model.
    """
    # Properties will be added by the mapper

    @classmethod
    def get(cls, vocabulary_id=None, **kwargs):
        """
        Get a vocabulary description object by vocabulary_id or other attributes.
        """
        query = meta.Session.query(cls)

        if vocabulary_id:
            return query.filter(cls.vocabulary_id == vocabulary_id).first()

        return query.filter_by(**kwargs).first()

    @classmethod
    def create(cls, vocabulary_id, description_el=None, description_en=None):
        """
        Create a new vocabulary description object.
        """
        description = cls(
            vocabulary_id=vocabulary_id,
            description_el=description_el,
            description_en=description_en
        )

        meta.Session.add(description)
        meta.Session.commit()

        return description

    @classmethod
    def update(cls, vocabulary_id, description_el=None, description_en=None):
        """
        Update an existing vocabulary description object.
        """
        description = cls.get(vocabulary_id=vocabulary_id)

        if not description:
            return cls.create(
                vocabulary_id=vocabulary_id,
                description_el=description_el,
                description_en=description_en
            )

        # Always update the fields, even if they're None
        # This ensures that fields can be cleared (set to NULL in the database)
        description.description_el = description_el
        description.description_en = description_en

        meta.Session.add(description)
        meta.Session.commit()

        return description

def get_vocabulary_description(vocabulary_id):
    """
    Get description for a vocabulary.

    Args:
        vocabulary_id: The ID of the vocabulary.

    Returns:
        A dictionary containing the vocabulary description, or None if no description exists.
    """
    description = VocabularyDescription.get(vocabulary_id=vocabulary_id)
    if not description:
        return None

    return {
        'description_el': description.description_el,
        'description_en': description.description_en
    }


# Map the class to the table
meta.registry.map_imperatively(VocabularyDescription, vocabulary_description_table, properties={
    'vocabulary': relationship(CkanVocabulary, backref='description')
})
