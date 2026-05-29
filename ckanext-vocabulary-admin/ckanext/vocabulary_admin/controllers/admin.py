# -*- coding: utf-8 -*-
import ckan.lib.base as base
import ckan.model as model
import ckan.plugins.toolkit as toolkit
from ckan.common import _, g, request
from flask import render_template, redirect, url_for, flash

from ckanext.vocabulary_admin.cache import invalidate_vocabulary_cache
from ckanext.vocabulary_admin.model import vocabulary as vocabulary_model
from ckanext.vocabulary_admin.model.tag_metadata import VocabularyTagMetadata
from ckanext.vocabulary_admin.model.vocabulary_description import VocabularyDescription
from ckanext.vocabulary_admin.tag_usage_guard import check_tag_deactivation_allowed
from ckanext.vocabulary_admin.vocabulary_rules import (
    get_vocabulary_by_id_or_name,
    is_protected_vocabulary,
    parse_is_active_values,
)


_PROTECTED_IS_ACTIVE_ERROR = (
    u'Δεν επιτρέπεται η αλλαγή κατάστασης της ετικέτας για αυτό το λεξιλόγιο.'
)


_PROTECTED_TAG_CREATE_ERROR = (
    u'Δεν επιτρέπεται η δημιουργία νέας ετικέτας για αυτό το λεξιλόγιο.'
)


def _metadata_is_active(tag_metadata):
    if not tag_metadata:
        return True
    return bool(tag_metadata.is_active)


def _build_deactivation_usage_error(usage_result):
    dataset_count = usage_result['counts']['datasets'].get(
        'total_with_resource_usage',
        usage_result['counts']['datasets']['total'],
    )
    organization_count = usage_result['counts']['organizations']['total']
    has_organization_targets = usage_result.get('has_organization_targets', False)
    dataset_label = (
        _('σύνολο δεδομένων / υπηρεσία')
        if dataset_count == 1
        else _('σύνολα δεδομένων / υπηρεσίες')
    )
    organization_label = (
        _('οργανισμό')
        if organization_count == 1
        else _('οργανισμούς')
    )

    if has_organization_targets:
        message = _(
            u'Δεν μπορεί να γίνει απενεργοποίηση: χρησιμοποιείται σε {0} {1}, {2} {3}.'
        ).format(
            dataset_count,
            dataset_label,
            organization_count,
            organization_label,
        )
    else:
        message = _(
            u'Δεν μπορεί να γίνει απενεργοποίηση: χρησιμοποιείται σε {0} {1}.'
        ).format(
            dataset_count,
            dataset_label,
        )
    return message


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

            invalidate_vocabulary_cache()

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

    requested_is_active = True
    selected_vocabulary_id = request.form.get('vocabulary_id', '').strip()
    selected_vocabulary = get_vocabulary_by_id_or_name(selected_vocabulary_id)
    selected_vocabulary_name = selected_vocabulary.name if selected_vocabulary else None
    selected_is_protected = is_protected_vocabulary(selected_vocabulary_name)

    if request.method == 'POST':
        # Get form data
        name = request.form.get('name', '').strip()
        vocabulary_id = request.form.get('vocabulary_id', '').strip()
        value_uri = request.form.get('value_uri', '').strip()
        order_index_raw = request.form.get('order_index', '').strip()
        label_el = request.form.get('label_el', '').strip()
        label_en = request.form.get('label_en', '').strip()
        description_el = request.form.get('description_el', '').strip()
        description_en = request.form.get('description_en', '').strip()
        requested_is_active = parse_is_active_values(
            request.form.getlist('is_active'),
            default=True
        )

        selected_vocabulary = get_vocabulary_by_id_or_name(vocabulary_id)
        selected_vocabulary_name = selected_vocabulary.name if selected_vocabulary else None
        selected_is_protected = is_protected_vocabulary(selected_vocabulary_name)

        if not name:
            flash(_('Please enter a name for the tag'), 'error')
            return render_template(
                'admin/tag_create.html',
                vocabularies=vocabularies,
                is_protected_vocabulary=selected_is_protected,
                requested_is_active=requested_is_active,
                selected_vocabulary_id=vocabulary_id,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

        if not vocabulary_id:
            flash(_('Please select a vocabulary'), 'error')
            return render_template(
                'admin/tag_create.html',
                vocabularies=vocabularies,
                is_protected_vocabulary=selected_is_protected,
                requested_is_active=requested_is_active,
                selected_vocabulary_id=vocabulary_id,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

        # Hard block: δεν επιτρέπεται νέα ετικέτα σε protected vocabulary
        if selected_is_protected:
            flash(_(_PROTECTED_TAG_CREATE_ERROR), 'error')
            return render_template(
                'admin/tag_create.html',
                vocabularies=vocabularies,
                is_protected_vocabulary=False,
                requested_is_active=True,
                selected_vocabulary_id='',
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

        order_index = None
        if order_index_raw:
            try:
                order_index = int(order_index_raw)
            except ValueError:
                flash(_('The "Order index" must be an integer'), 'error')
                return render_template(
                    'admin/tag_create.html',
                    vocabularies=vocabularies,
                    is_protected_vocabulary=selected_is_protected,
                    requested_is_active=requested_is_active,
                    selected_vocabulary_id=vocabulary_id,
                    protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
                )

        try:
            data_dict = {
                'name': name,
                'vocabulary_id': vocabulary_id
            }
            tag = toolkit.get_action('tag_create')(context, data_dict)

            VocabularyTagMetadata.create(
                tag_id=tag['id'],
                value_uri=value_uri if value_uri else None,
                label_el=label_el if label_el else None,
                label_en=label_en if label_en else None,
                description_el=description_el if description_el else None,
                description_en=description_en if description_en else None,
                is_active=requested_is_active,
                order_index=order_index
            )

            invalidate_vocabulary_cache()

            flash(_('Tag created successfully'), 'alert-success')
            return redirect(url_for('vocabularyadmin.vocabulary_admin'))
        except toolkit.ValidationError as e:
            flash(_('Error creating tag: {0}').format(str(e)), 'error')
            return render_template(
                'admin/tag_create.html',
                vocabularies=vocabularies,
                is_protected_vocabulary=selected_is_protected,
                requested_is_active=requested_is_active,
                selected_vocabulary_id=vocabulary_id,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

    return render_template(
        'admin/tag_create.html',
        vocabularies=vocabularies,
        is_protected_vocabulary=selected_is_protected,
        requested_is_active=requested_is_active,
        selected_vocabulary_id=selected_vocabulary_id,
        protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
    )


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
    current_is_active = _metadata_is_active(tag_metadata)
    current_vocabulary = model.Vocabulary.get(tag.vocabulary_id) if tag.vocabulary_id else None
    current_vocabulary_name = current_vocabulary.name if current_vocabulary else None
    tag_is_protected_vocabulary = is_protected_vocabulary(current_vocabulary_name)
    requested_is_active = current_is_active

    if request.method == 'POST':
        # Get form data
        name = request.form.get('name', '').strip()
        vocabulary_id = request.form.get('vocabulary_id', '').strip()
        value_uri = request.form.get('value_uri', '').strip()
        order_index_raw = request.form.get('order_index', '').strip()
        label_el = request.form.get('label_el', '').strip()
        label_en = request.form.get('label_en', '').strip()
        description_el = request.form.get('description_el', '').strip()
        description_en = request.form.get('description_en', '').strip()
        requested_is_active = parse_is_active_values(
            request.form.getlist('is_active'),
            default=current_is_active
        )
        submitted_vocabulary = get_vocabulary_by_id_or_name(vocabulary_id)
        submitted_vocabulary_name = submitted_vocabulary.name if submitted_vocabulary else None
        submitted_is_protected_vocabulary = is_protected_vocabulary(submitted_vocabulary_name)
        effective_is_protected_vocabulary = (
            tag_is_protected_vocabulary or submitted_is_protected_vocabulary
        )

        if not name:
            flash(_('Please enter a name for the tag'), 'error')
            return render_template(
                'admin/tag_edit.html',
                tag=tag,
                tag_metadata=tag_metadata,
                vocabularies=vocabularies,
                is_protected_vocabulary=effective_is_protected_vocabulary,
                current_is_active=current_is_active,
                requested_is_active=requested_is_active,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

        if not vocabulary_id:
            flash(_('Please select a vocabulary'), 'error')
            return render_template(
                'admin/tag_edit.html',
                tag=tag,
                tag_metadata=tag_metadata,
                vocabularies=vocabularies,
                is_protected_vocabulary=effective_is_protected_vocabulary,
                current_is_active=current_is_active,
                requested_is_active=requested_is_active,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

        if effective_is_protected_vocabulary and requested_is_active != current_is_active:
            return render_template(
                'admin/tag_edit.html',
                tag=tag,
                tag_metadata=tag_metadata,
                vocabularies=vocabularies,
                is_protected_vocabulary=effective_is_protected_vocabulary,
                current_is_active=current_is_active,
                requested_is_active=current_is_active,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR),
                is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

        if (not effective_is_protected_vocabulary and current_is_active and
                not requested_is_active):
            deactivation_check = check_tag_deactivation_allowed(tag_id)
            if deactivation_check.get('blocked'):
                deactivation_error_message = _build_deactivation_usage_error(deactivation_check)
                return render_template(
                    'admin/tag_edit.html',
                    tag=tag,
                    tag_metadata=tag_metadata,
                    vocabularies=vocabularies,
                    is_protected_vocabulary=effective_is_protected_vocabulary,
                    current_is_active=current_is_active,
                    requested_is_active=current_is_active,
                    protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR),
                    is_active_error_message=deactivation_error_message
                )

        # Validate order_index (optional integer)
        order_index = None
        if order_index_raw:
            try:
                order_index = int(order_index_raw)
            except ValueError:
                flash(_('The "Order index" must be an integer'), 'error')
                return render_template('admin/tag_edit.html',
                                       tag=tag,
                                       tag_metadata=tag_metadata,
                                       vocabularies=vocabularies,
                                       is_protected_vocabulary=effective_is_protected_vocabulary,
                                       current_is_active=current_is_active,
                                       requested_is_active=requested_is_active,
                                       protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR))

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
                is_active=requested_is_active,
                order_index=order_index
            )

            invalidate_vocabulary_cache()

            flash(_('Tag updated successfully'), 'alert-success')
            return redirect(url_for('vocabularyadmin.vocabulary_admin'))
        except toolkit.ValidationError as e:
            flash(_('Error updating tag: {0}').format(str(e)), 'error')
            return render_template(
                'admin/tag_edit.html',
                tag=tag,
                tag_metadata=tag_metadata,
                vocabularies=vocabularies,
                is_protected_vocabulary=effective_is_protected_vocabulary,
                current_is_active=current_is_active,
                requested_is_active=requested_is_active,
                protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
            )

    # GET request - display the form with pre-filled values
    return render_template(
        'admin/tag_edit.html',
        tag=tag,
        tag_metadata=tag_metadata,
        vocabularies=vocabularies,
        is_protected_vocabulary=tag_is_protected_vocabulary,
        current_is_active=current_is_active,
        requested_is_active=requested_is_active,
        protected_is_active_error_message=_(_PROTECTED_IS_ACTIVE_ERROR)
    )


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
        tag_order_raw = request.form.get('tag_order', '').strip()

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

            # Update tags order if provided
            if tag_order_raw:
                tag_ids = [tid for tid in tag_order_raw.split(',') if tid]
                order_value = 1
                for tid in tag_ids:
                    VocabularyTagMetadata.update(
                        tag_id=tid,
                        order_index=order_value
                    )
                    order_value += 1

            invalidate_vocabulary_cache()

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
    # Check if vocabulary deletion is enabled
    if not toolkit.asbool(toolkit.config.get('ckanext.vocabulary_admin.enable_vocabulary_delete', False)):
        return toolkit.abort(403, _('Vocabulary deletion is disabled by configuration.'))

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
    # Check if tag deletion is enabled
    if not toolkit.asbool(toolkit.config.get('ckanext.vocabulary_admin.enable_tag_delete', False)):
        return toolkit.abort(403, _('Tag deletion is disabled by configuration.'))

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
