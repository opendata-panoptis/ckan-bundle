# -*- coding: utf-8 -*-
"""
Helper functions for the vocabulary admin extension.
"""
import logging
import ckan.plugins.toolkit as toolkit
from ckan.lib.helpers import lang
from ckanext.vocabulary_admin.model import vocabulary as vocabulary_model
from ckanext.vocabulary_admin.model.tag_metadata import get_tag_metadata as _get_tag_metadata
from ckanext.vocabulary_admin.model.vocabulary_description import get_vocabulary_description as _get_vocabulary_description

# Δημιουργούμε ένα logger για αυτό το αρχείο
log = logging.getLogger(__name__)

# Re-export functions from vocabulary_model
get_vocabularies = vocabulary_model.get_vocabularies
get_vocabulary = vocabulary_model.get_vocabulary
get_tags = vocabulary_model.get_tags
get_vocabulary_count = vocabulary_model.get_vocabulary_count
get_tag_count = vocabulary_model.get_tag_count

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
        log.debug(u'Fetching tags for vocabulary: {0}'.format(vocabulary_id_or_name))
        vocabulary_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
            {}, {'id': vocabulary_id_or_name}
        )
        tags = vocabulary_data.get('tags', [])

        # Use value_uri for the value if available, otherwise fall back to tag name
        choices = [{'value': tag.get('value_uri', tag['name']), 'label': _get_label_by_language(tag)} for tag in tags]

        log.debug(u'Found {0} choices for vocabulary {1}'.format(len(choices), vocabulary_id_or_name))

        return choices

    except toolkit.ObjectNotFound:
        # If the vocabulary is not found, return an empty list
        log.warning(
            u'Vocabulary not found: "{0}"'.format(vocabulary_id_or_name)
        )
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
