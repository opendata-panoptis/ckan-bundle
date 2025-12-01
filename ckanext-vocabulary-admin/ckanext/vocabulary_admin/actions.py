# -*- coding: utf-8 -*-
"""
Action functions for the vocabulary admin extension.
"""
import logging
from ckan import model
import ckan.plugins.toolkit as toolkit
import ckan.lib.dictization.model_save as model_save
import ckan.lib.dictization.model_dictize as model_dictize
from ckan.logic import validate, ValidationError, NotFound
import ckan.logic.schema as schema_
from ckan.common import _

from ckanext.vocabulary_admin.model.tag_metadata import VocabularyTagMetadata
from ckanext.vocabulary_admin.model.vocabulary_description import VocabularyDescription
from ckanext.vocabulary_admin.model import tag_metadata as tag_metadata_model
from ckanext.vocabulary_admin.model import vocabulary_description as vocab_desc_model

log = logging.getLogger(__name__)

def tag_update(context, data_dict):
    """
    Update a tag.

    You must be a sysadmin to update tags.

    :param id: the id or name of the tag to update
    :type id: string
    :param name: the name for the tag, a string between 2 and 100
        characters long containing only alphanumeric characters,
        spaces and the characters ``-``,
        ``_`` and ``.``, e.g. ``'Jazz'``
    :type name: string
    :param vocabulary_id: the id of the vocabulary that the tag
        belongs to, e.g. the id of vocabulary ``'Genre'``
    :type vocabulary_id: string

    :returns: the updated tag
    :rtype: dictionary
    """
    model = context['model']

    toolkit.check_access('tag_update', context, data_dict)

    # Get the tag
    tag_id = data_dict.get('id')
    if not tag_id:
        raise ValidationError({'id': 'Tag id not provided'})

    tag = model.Tag.get(tag_id)
    if not tag:
        raise NotFound('Tag not found')

    # Add the tag to the context so that tag_dict_save knows to update it
    context['tag'] = tag

    # Create a custom schema for tag updates that allows the 'id' field
    # and doesn't check if the tag already exists in the vocabulary
    schema = {
        'id': [],  # Allow id field
        'name': schema_.default_tags_schema()['name'],
        'vocabulary_id': [],  # Allow vocabulary_id field without validation
        'revision_timestamp': [toolkit.get_validator('ignore')],
        'state': [toolkit.get_validator('ignore')],
        'display_name': [toolkit.get_validator('ignore')],
    }

    data, errors = toolkit.navl_validate(data_dict, schema, context)
    if errors:
        raise ValidationError(errors)

    tag = model_save.tag_dict_save(data_dict, context)

    if not context.get('defer_commit'):
        model.repo.commit()

    log.info("Updated tag '%s'", tag)
    return model_dictize.tag_dictize(tag, context)

@toolkit.auth_allow_anonymous_access
def tag_update_auth(context, data_dict):
    """
    Authorization check for tag_update.

    Only sysadmins can update tags.
    """
    # Check if the user is a sysadmin
    return {'success': toolkit.check_access('sysadmin', context, data_dict)}


def vocabularyadmin_vocabulary_delete(context, data_dict):
    """
    Διαγράφει ένα λεξιλόγιο και ΟΛΑ τα σχετικά δεδομένα (περιγραφή, tags, και metadata των tags).
    """
    # Έλεγχος δικαιωμάτων
    toolkit.check_access('vocabulary_delete', context, data_dict)

    vocab_id = toolkit.get_or_bust(data_dict, 'id')

    # Φέρνουμε το αντικείμενο του λεξιλογίου από τη βάση
    vocab_obj = model.Vocabulary.get(vocab_id)
    if not vocab_obj:
        raise toolkit.ObjectNotFound(toolkit._('Vocabulary not found'))

    # Παίρνουμε ΟΛΑ τα tags που συνδέονται με αυτό το λεξιλόγιο
    # Convert query to list to avoid issues with iterating over a changing collection
    tags_to_delete = list(vocab_obj.tags)
    tag_ids_to_delete = [tag.id for tag in tags_to_delete]

    # --- Εκτελούμε τις διαγραφές με τη σωστή σειρά ---

    # 1. Διαγράφουμε πρώτα τα metadata των tags (από τον πίνακα vocabulary_tag_metadata)
    if tag_ids_to_delete:
        meta_query = model.Session.query(tag_metadata_model.VocabularyTagMetadata)
        meta_query.filter(tag_metadata_model.VocabularyTagMetadata.tag_id.in_(tag_ids_to_delete)).delete(synchronize_session=False)
        log.info(u"Deleted VocabularyTagMetadata for tags in vocabulary {0}".format(vocab_id))

    # 2. Διαγράφουμε τις ίδιες τις ετικέτες (από τον πίνακα tag)
    for tag in tags_to_delete:
        model.Session.delete(tag)
    log.info(u"Deleted {0} tags for vocabulary {1}".format(len(tags_to_delete), vocab_id))

    # 3. Διαγράφουμε την περιγραφή του λεξιλογίου (από τον πίνακα vocabulary_description)
    desc_obj = vocab_desc_model.VocabularyDescription.get(vocabulary_id=vocab_id)
    if desc_obj:
        model.Session.delete(desc_obj)
        log.info(u"Deleted description for vocabulary {0}".format(vocab_id))

    # 4. Τέλος, διαγράφουμε το ίδιο το λεξιλόγιο (από τον πίνακα vocabulary)
    model.Session.delete(vocab_obj)
    log.info(u"Deleted vocabulary {0}".format(vocab_id))

    # Αποθηκεύουμε όλες τις αλλαγές στη βάση δεδομένων
    model.Session.commit()

    return {'success': True}


@toolkit.auth_allow_anonymous_access
def vocabularyadmin_vocabulary_delete_auth(context, data_dict):
    """
    Authorization check for vocabularyadmin_vocabulary_delete.

    Only sysadmins can delete vocabularies.
    """
    # Check if the user is a sysadmin
    return {'success': toolkit.check_access('sysadmin', context, data_dict)}


def vocabularyadmin_tag_delete(context, data_dict):
    """
    Delete a tag and its associated metadata in a single transaction.

    This function deletes both the tag and its custom metadata in a single
    transaction, ensuring data consistency.

    You must be a sysadmin to delete tags.

    :param id: the id or name of the tag to delete
    :type id: string
    :param vocabulary_id: the id or name of the vocabulary that the tag belongs
        to (optional, default: None)
    :type vocabulary_id: string

    :returns: None
    """
    model = context['model']
    toolkit.check_access('sysadmin', context, data_dict)
    tag_id = toolkit.get_or_bust(data_dict, 'id')
    tag_obj = model.Tag.get(tag_id, data_dict.get('vocabulary_id'))
    if not tag_obj:
        raise toolkit.ObjectNotFound(_('Tag not found.'))

    # 1. Βρίσκουμε και προετοιμάζουμε για διαγραφή τα metadata
    tag_metadata = VocabularyTagMetadata.get(tag_id=tag_obj.id)
    if tag_metadata:
        model.Session.delete(tag_metadata)
        log.info(f"Marked VocabularyTagMetadata for deletion for tag {tag_obj.id}")

    # 2. Προετοιμάζουμε για διαγραφή το ίδιο το tag
    model.Session.delete(tag_obj)
    log.info(f"Marked tag {tag_obj.id} for deletion.")

    # 3. Κάνουμε commit ΜΙΑ φορά στο τέλος, για να γίνουν όλες οι αλλαγές μαζί.
    try:
        model.Session.commit()
        log.info(f"Successfully deleted tag {tag_obj.id} and its metadata.")
    except Exception as e:
        model.Session.rollback()
        raise toolkit.ValidationError(f"Error committing deletions for tag {tag_obj.id}: {e}")


@toolkit.auth_allow_anonymous_access
def vocabularyadmin_tag_delete_auth(context, data_dict):
    """
    Authorization check for vocabularyadmin_tag_delete.

    Only sysadmins can delete tags.
    """
    # Check if the user is a sysadmin
    return {'success': toolkit.check_access('sysadmin', context, data_dict)}


@toolkit.side_effect_free
def vocabularyadmin_vocabulary_show(context, data_dict):
    """
    Return a single tag vocabulary, but filter the tags
    to include only those that are active (`is_active=true`).
    Also enriches the tags with their metadata.

    Args:
        context: The context dict
        data_dict: Must contain an 'id' key with the id or name of the vocabulary

    Returns:
        A dictionary containing the vocabulary with only active tags, enriched with metadata
    """
    # Call the original, core CKAN action to get the basic data
    vocabulary_data = toolkit.get_action('vocabulary_show')(context, data_dict)

    enriched_active_tags = []
    for tag in vocabulary_data.get('tags', []):
        # For each tag, get its custom metadata
        metadata = tag_metadata_model.get_tag_metadata(tag['id'])

        # Keep the tag only if it's active (or if it has no metadata,
        # consider it active for compatibility reasons)
        if not metadata or metadata.get('is_active', False) is True:
            # Merge the metadata with the tag data
            if metadata:
                tag.update(metadata)
            enriched_active_tags.append(tag)

    vocabulary_data['tags'] = enriched_active_tags

    return vocabulary_data
