# -*- coding: utf-8 -*-
import ckan.lib.base as base
import ckan.model as model
import ckan.plugins.toolkit as toolkit
from ckan.common import _, g, request
from flask import render_template, redirect, url_for, flash

from ckanext.vocabulary_admin.model import vocabulary as vocabulary_model
from ckanext.vocabulary_admin.model.tag_metadata import VocabularyTagMetadata
from ckanext.vocabulary_admin.model.vocabulary_description import VocabularyDescription


def index():
    """
    Display the main vocabulary management page.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    # Get all vocabularies
    vocabularies = vocabulary_model.get_vocabularies()

    # Prepare data for the template
    data = {
        'vocabularies': vocabularies
    }

    # Render the template
    return render_template('admin/vocabulary_management_index.html',
                          data=data)


def create_vocabulary():
    """
    Display a form for creating a new vocabulary and handle form submission.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    if request.method == 'POST':
        # Get form data
        name = request.form.get('name', '').strip()
        description_el = request.form.get('description_el', '').strip()
        description_en = request.form.get('description_en', '').strip()

        if not name:
            flash(_('Please enter a name for the vocabulary'), 'error')
            return render_template('admin/vocabulary_create.html')

        # Create vocabulary
        try:
            data_dict = {'name': name}
            vocabulary = toolkit.get_action('vocabulary_create')(context, data_dict)

            # Create vocabulary description
            if description_el or description_en:
                VocabularyDescription.create(
                    vocabulary_id=vocabulary['id'],
                    description_el=description_el if description_el else None,
                    description_en=description_en if description_en else None
                )

            flash(_('Vocabulary created successfully'), 'alert-success')
            return redirect(url_for('vocabularyadmin.vocabulary_admin'))
        except toolkit.ValidationError as e:
            flash(_('Error creating vocabulary: {0}').format(str(e)), 'error')
            return render_template('admin/vocabulary_create.html')

    # GET request - display the form
    return render_template('admin/vocabulary_create.html')


def create_tag():
    """
    Display a form for creating a new tag and handle form submission.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    # Get all vocabularies for the dropdown
    vocabularies = vocabulary_model.get_vocabularies()

    if request.method == 'POST':
        # Get form data
        name = request.form.get('name', '').strip()
        vocabulary_id = request.form.get('vocabulary_id', '').strip()
        value_uri = request.form.get('value_uri', '').strip()
        label_el = request.form.get('label_el', '').strip()
        label_en = request.form.get('label_en', '').strip()
        description_el = request.form.get('description_el', '').strip()
        description_en = request.form.get('description_en', '').strip()
        is_active = 'is_active' in request.form

        if not name:
            flash(_('Please enter a name for the tag'), 'error')
            return render_template('admin/tag_create.html', vocabularies=vocabularies)

        if not vocabulary_id:
            flash(_('Please select a vocabulary'), 'error')
            return render_template('admin/tag_create.html', vocabularies=vocabularies)

        # Create tag
        try:
            data_dict = {
                'name': name,
                'vocabulary_id': vocabulary_id
            }
            tag = toolkit.get_action('tag_create')(context, data_dict)

            # Create tag metadata
            VocabularyTagMetadata.create(
                tag_id=tag['id'],
                value_uri=value_uri if value_uri else None,
                label_el=label_el if label_el else None,
                label_en=label_en if label_en else None,
                description_el=description_el if description_el else None,
                description_en=description_en if description_en else None,
                is_active=is_active
            )

            flash(_('Tag created successfully'), 'alert-success')
            return redirect(url_for('vocabularyadmin.vocabulary_admin'))
        except toolkit.ValidationError as e:
            flash(_('Error creating tag: {0}').format(str(e)), 'error')
            return render_template('admin/tag_create.html', vocabularies=vocabularies)

    # GET request - display the form
    return render_template('admin/tag_create.html', vocabularies=vocabularies)


def edit_tag(tag_id):
    """
    Display a form for editing an existing tag and handle form submission.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    # Get the tag
    try:
        tag = model.Tag.get(tag_id)
        if not tag:
            return toolkit.abort(404, _('Tag not found'))
    except Exception as e:
        return toolkit.abort(404, _('Tag not found: {0}').format(str(e)))

    # Get all vocabularies for the dropdown
    vocabularies = vocabulary_model.get_vocabularies()

    # Get tag metadata
    tag_metadata = VocabularyTagMetadata.get(tag_id=tag_id)

    if request.method == 'POST':
        # Get form data
        name = request.form.get('name', '').strip()
        vocabulary_id = request.form.get('vocabulary_id', '').strip()
        value_uri = request.form.get('value_uri', '').strip()
        label_el = request.form.get('label_el', '').strip()
        label_en = request.form.get('label_en', '').strip()
        description_el = request.form.get('description_el', '').strip()
        description_en = request.form.get('description_en', '').strip()
        is_active = 'is_active' in request.form

        if not name:
            flash(_('Please enter a name for the tag'), 'error')
            return render_template('admin/tag_edit.html', 
                                  tag=tag, 
                                  tag_metadata=tag_metadata, 
                                  vocabularies=vocabularies)

        if not vocabulary_id:
            flash(_('Please select a vocabulary'), 'error')
            return render_template('admin/tag_edit.html', 
                                  tag=tag, 
                                  tag_metadata=tag_metadata, 
                                  vocabularies=vocabularies)

        # Update tag
        try:
            data_dict = {
                'id': tag_id,
                'name': name,
                'vocabulary_id': vocabulary_id
            }
            updated_tag = toolkit.get_action('tag_update')(context, data_dict)

            # Update tag metadata
            VocabularyTagMetadata.update(
                tag_id=tag_id,
                value_uri=value_uri if value_uri else None,
                label_el=label_el if label_el else None,
                label_en=label_en if label_en else None,
                description_el=description_el if description_el else None,
                description_en=description_en if description_en else None,
                is_active=is_active
            )

            flash(_('Tag updated successfully'), 'alert-success')
            return redirect(url_for('vocabularyadmin.vocabulary_admin'))
        except toolkit.ValidationError as e:
            flash(_('Error updating tag: {0}').format(str(e)), 'error')
            return render_template('admin/tag_edit.html', 
                                  tag=tag, 
                                  tag_metadata=tag_metadata, 
                                  vocabularies=vocabularies)

    # GET request - display the form with pre-filled values
    return render_template('admin/tag_edit.html', 
                          tag=tag, 
                          tag_metadata=tag_metadata, 
                          vocabularies=vocabularies)


def edit_vocabulary(vocabulary_id):
    """
    Display a form for editing an existing vocabulary and handle form submission.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    # Get the vocabulary
    try:
        vocabulary = vocabulary_model.get_vocabulary(vocabulary_id)
        if not vocabulary:
            return toolkit.abort(404, _('Vocabulary not found'))
    except Exception as e:
        return toolkit.abort(404, _('Vocabulary not found: {0}').format(str(e)))

    # Get vocabulary description
    vocabulary_description = VocabularyDescription.get(vocabulary_id=vocabulary_id)

    if request.method == 'POST':
        # Get form data
        name = request.form.get('name', '').strip()
        description_el = request.form.get('description_el', '').strip()
        description_en = request.form.get('description_en', '').strip()

        if not name:
            flash(_('Please enter a name for the vocabulary'), 'error')
            return render_template('admin/vocabulary_edit.html', 
                                  vocabulary=vocabulary, 
                                  vocabulary_description=vocabulary_description)

        # Update vocabulary
        try:
            data_dict = {
                'id': vocabulary_id,
                'name': name
            }
            updated_vocabulary = toolkit.get_action('vocabulary_update')(context, data_dict)

            # Update vocabulary description
            # Always update description fields, even if they're empty
            VocabularyDescription.update(
                vocabulary_id=vocabulary_id,
                description_el=description_el if description_el else None,
                description_en=description_en if description_en else None
            )

            flash(_('Vocabulary updated successfully'), 'alert-success')
            return redirect(url_for('vocabularyadmin.vocabulary_admin'))
        except toolkit.ValidationError as e:
            flash(_('Error updating vocabulary: {0}').format(str(e)), 'error')
            return render_template('admin/vocabulary_edit.html', 
                                  vocabulary=vocabulary, 
                                  vocabulary_description=vocabulary_description)

    # GET request - display the form with pre-filled values
    return render_template('admin/vocabulary_edit.html', 
                          vocabulary=vocabulary, 
                          vocabulary_description=vocabulary_description)


def delete_vocabulary(vocabulary_id):
    """
    Delete a vocabulary and all its associated tags.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    # Get the vocabulary
    try:
        vocabulary = vocabulary_model.get_vocabulary(vocabulary_id)
        if not vocabulary:
            return toolkit.abort(404, _('Vocabulary not found'))
    except Exception as e:
        return toolkit.abort(404, _('Vocabulary not found: {0}').format(str(e)))

    # Delete the vocabulary and its associated data
    try:
        data_dict = {'id': vocabulary_id}
        toolkit.get_action('vocabularyadmin_vocabulary_delete')(context, data_dict)
        flash(_('Vocabulary and all its tags deleted successfully'), 'alert-success')
    except toolkit.ValidationError as e:
        flash(_('Error deleting vocabulary: {0}').format(str(e)), 'error')
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Not authorized to delete vocabulary'))
    except toolkit.ObjectNotFound:
        flash(_('Vocabulary not found'), 'error')
    except Exception as e:
        flash(_('Error deleting vocabulary: {0}').format(str(e)), 'error')

    return redirect(url_for('vocabularyadmin.vocabulary_admin'))


def delete_tag(tag_id):
    """
    Delete a tag and its associated metadata.
    """
    # Check if user has admin permissions
    context = {'model': model, 'user': g.user}
    try:
        toolkit.check_access('sysadmin', context, {})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Need to be system administrator to administer'))

    # Get the tag
    try:
        tag = model.Tag.get(tag_id)
        if not tag:
            return toolkit.abort(404, _('Tag not found'))
    except Exception as e:
        return toolkit.abort(404, _('Tag not found: {0}').format(str(e)))

    # Delete the tag and its associated metadata
    try:
        data_dict = {'id': tag_id}
        if tag.vocabulary_id:
            data_dict['vocabulary_id'] = tag.vocabulary_id
        toolkit.get_action('vocabularyadmin_tag_delete')(context, data_dict)
        flash(_('Tag deleted successfully'), 'alert-success')
    except toolkit.ValidationError as e:
        flash(_('Error deleting tag: {0}').format(str(e)), 'error')
    except toolkit.NotAuthorized:
        return toolkit.abort(403, _('Not authorized to delete tag'))
    except toolkit.ObjectNotFound:
        flash(_('Tag not found'), 'error')
    except Exception as e:
        flash(_('Error deleting tag: {0}').format(str(e)), 'error')

    return redirect(url_for('vocabularyadmin.vocabulary_admin'))
