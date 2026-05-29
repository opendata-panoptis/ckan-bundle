# -*- coding: utf-8 -*-
"""
Helper functions for the vocabulary admin extension.
"""
import logging
import threading
import ckan.plugins.toolkit as toolkit
from ckan.lib.helpers import lang
from ckanext.vocabulary_admin.model import vocabulary as vocabulary_model
from ckanext.vocabulary_admin import cache as vocabulary_cache
from ckanext.vocabulary_admin.model.tag_metadata import get_tag_metadata as _get_tag_metadata
from ckanext.vocabulary_admin.model.vocabulary_description import get_vocabulary_description as _get_vocabulary_description
from ckanext.vocabulary_admin.vocabulary_rules import is_protected_vocabulary

# Δημιουργούμε ένα logger για αυτό το αρχείο
log = logging.getLogger(__name__)

# Re-export functions from vocabulary_model
get_vocabularies = vocabulary_model.get_vocabularies
get_vocabulary = vocabulary_model.get_vocabulary
get_tags = vocabulary_model.get_tags
get_vocabulary_count = vocabulary_model.get_vocabulary_count
get_tag_count = vocabulary_model.get_tag_count

_VOCAB_CHOICES_CACHE = {}
_VOCAB_CHOICES_CACHE_VERSION = None
_VOCAB_CHOICES_CACHE_LOCK = threading.Lock()

def _get_label_by_language(tag):
    try:
        current_lang = lang()
    except RuntimeError:
        # If we're outside of request context, default to Greek
        current_lang = 'el'

    if current_lang == 'el':
        return tag.get('label_el') or tag.get('label_en') or tag.get('display_name')
    elif current_lang == 'en':
        return tag.get('label_en') or tag.get('display_name')

    return tag.get('display_name')

def vocabularyadmin_get_tags_for_scheming(field):
    """
    Retrieves tags from a specific vocabulary and formats them
    in the structure required by ckanext-scheming for the choices_helper.

    The 'field' argument is the entire field definition dictionary from the
    scheming YAML file.
    """
    # Βήμα 1: Παίρνουμε το λεξικό με τις παραμέτρους που ορίσαμε στο YAML.
    kwargs = field.get('form_choices_helper_kwargs', {})

    # Βήμα 2: Παίρνουμε το όνομα του λεξιλογίου από τις παραμέτρους.
    vocabulary_id_or_name = kwargs.get('vocabulary_id_or_name')

    if not vocabulary_id_or_name:
        log.warning(u"choices_helper `vocabularyadmin_get_tags_for_scheming` was called on field "
                    u"'{0}' but `vocabulary_id_or_name` was not provided in "
                    u"`form_choices_helper_kwargs`.".format(field.get('field_name')))
        return []

    try:
        current_lang = lang()
    except RuntimeError:
        current_lang = 'el'

    global _VOCAB_CHOICES_CACHE_VERSION
    cache_version = vocabulary_cache.get_vocabulary_cache_version()
    with _VOCAB_CHOICES_CACHE_LOCK:
        if _VOCAB_CHOICES_CACHE_VERSION != cache_version:
            _VOCAB_CHOICES_CACHE.clear()
            _VOCAB_CHOICES_CACHE_VERSION = cache_version

        cache_key = (vocabulary_id_or_name, current_lang)
        cached_choices = _VOCAB_CHOICES_CACHE.get(cache_key)
        if cached_choices is not None:
            return cached_choices

    try:
        log.debug(u'Fetching tags for vocabulary: {0}'.format(vocabulary_id_or_name))
        vocabulary_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
            {}, {'id': vocabulary_id_or_name}
        )
        tags = vocabulary_data.get('tags', [])

        # Use value_uri for the value if available, otherwise fall back to tag name
        choices = [{'value': tag.get('value_uri', tag['name']), 'label': _get_label_by_language(tag)} for tag in tags]

        log.debug(u'Found {0} choices for vocabulary {1}'.format(len(choices), vocabulary_id_or_name))

        with _VOCAB_CHOICES_CACHE_LOCK:
            _VOCAB_CHOICES_CACHE[cache_key] = choices

        return choices

    except toolkit.ObjectNotFound:
        # Vocabulary missing - avoid noisy logs on repeated lookups
        log.info(u'Vocabulary not found: "{0}"'.format(vocabulary_id_or_name))
        return []
    except Exception:
        log.exception(
            u'vocabularyadmin_get_tags_for_scheming helper failed for vocabulary "{0}":'.format(
                vocabulary_id_or_name
            )
        )
        return []


def get_tag_metadata(tag_id):
    return _get_tag_metadata(tag_id)

def get_vocabulary_description(vocabulary_id):
    return _get_vocabulary_description(vocabulary_id)


def vocabularyadmin_is_vocabulary_delete_enabled():
    """Check if vocabulary deletion is enabled via ckan.ini config."""
    return toolkit.asbool(
        toolkit.config.get('ckanext.vocabulary_admin.enable_vocabulary_delete', False)
    )


def vocabularyadmin_is_tag_delete_enabled():
    """Check if tag deletion is enabled via ckan.ini config."""
    return toolkit.asbool(
        toolkit.config.get('ckanext.vocabulary_admin.enable_tag_delete', False)
    )


def vocabularyadmin_is_vocabulary_name_readonly():
    """Check if vocabulary name should be readonly on edit via ckan.ini config."""
    return toolkit.asbool(
        toolkit.config.get('ckanext.vocabulary_admin.vocabulary_name_readonly', True)
    )


def vocabularyadmin_is_tag_name_readonly():
    """Check if tag name should be readonly on edit via ckan.ini config."""
    return toolkit.asbool(
        toolkit.config.get('ckanext.vocabulary_admin.tag_name_readonly', True)
    )


def vocabularyadmin_is_tag_vocabulary_readonly():
    """Check if tag vocabulary selection should be readonly on edit via ckan.ini config."""
    return toolkit.asbool(
        toolkit.config.get('ckanext.vocabulary_admin.tag_vocabulary_readonly', True)
    )


def vocabularyadmin_is_tag_uri_readonly():
    """Check if tag value_uri should be readonly on edit via ckan.ini config."""
    return toolkit.asbool(
        toolkit.config.get('ckanext.vocabulary_admin.tag_uri_readonly', True)
    )


def vocabularyadmin_is_protected_vocabulary(vocabulary_name):
    """Check if a vocabulary has immutable is_active for its tags."""
    return is_protected_vocabulary(vocabulary_name)
