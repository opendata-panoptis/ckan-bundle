# !/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-contact
# Created by the Natural History Museum in London, UK
import logging
import socket
from datetime import datetime, timezone

from ckan import logic
from ckan.common import asbool, config
from ckan.lib import mailer
from ckan.lib.navl.dictization_functions import unflatten
from ckan.plugins import PluginImplementations, toolkit
from pyisemail import is_email

from ckanext.contact import recaptcha
from ckanext.contact.interfaces import IContact
from ckanext.contact.helpers import contact_accept_terms_enabled

log = logging.getLogger(__name__)


def validate(data_dict):
    """
    Validates the given data and recaptcha if necessary.

    :param data_dict: the request params as a dict
    :returns: a 3-tuple of errors, error summaries and a recaptcha error, in the event
        where no issues occur the return is ({}, {}, None)
    """
    errors = {}
    error_summary = {}
    # subject και τα επιπλέον περιγραφικά πεδία είναι προαιρετικά
    optional_fields = {
        'subject',
        'actor_type',
        'organization',
        'role',
        'subject_type',
        'related_reference',
        # fields used in the support tree (helper selections)
        'support_request_type',
        'support_question_scope',
        'support_data_topic',
        'support_subject_choice',
        # context flag (page/modal)
        'contact_context',
    }
    recaptcha_error = None

    accept_terms_enabled = contact_accept_terms_enabled()

    # Αν το submit προέρχεται από modal, δεν απαιτούμε accept_terms (ακόμα κι αν είναι enabled)
    contact_context = (data_dict.get('contact_context') or '').strip().lower()
    accept_terms_required = accept_terms_enabled and contact_context != 'modal'

    if not accept_terms_required:
        # If terms are not required in this context, do not require the field at all.
        optional_fields.add('accept_terms')

    # check each field to see if it has a value and if not, show and error
    for field, value in data_dict.items():
        # we know the save field is not necessary and may be empty so ignore it
        if field == 'save':
            continue
        # ignore optionals
        if field in optional_fields:
            continue
        if value is None or value == '':
            errors[field] = ['Missing Value']
            error_summary[field] = 'Missing value'

    # ensure ότι έχουν αποδεχθεί τους όρους χρήσης (μόνο όταν είναι required)
    if accept_terms_required and not data_dict.get('accept_terms'):
        errors['accept_terms'] = [
            toolkit._('You must accept the terms of use to continue.')
        ]
        error_summary['accept_terms'] = toolkit._(
            'You must accept the terms of use.'
        )

    # check the email address, if there is one and the config option isn't off
    if (
        toolkit.asbool(toolkit.config.get('ckanext.contact.check_email', True))
        and data_dict['email']
    ):
        if not is_email(data_dict['email'], check_dns=True):
            errors['email'] = ['Email address appears to be invalid']
            error_summary['email'] = 'Email address appears to be invalid'

    # only check the recaptcha if there are no errors
    if not errors:
        try:
            expected_action = toolkit.config.get('ckanext.contact.recaptcha_v3_action')
            # check the recaptcha value, this only does anything if recaptcha is setup
            recaptcha.check_recaptcha(
                data_dict.get('g-recaptcha-response', None), expected_action
            )
        except recaptcha.RecaptchaError as e:
            log.info(f'Recaptcha failed due to "{e}"')
            recaptcha_error = toolkit._('Recaptcha check failed, please try again.')

    return errors, error_summary, recaptcha_error


def build_subject(
    subject=None, default='Contact/Question from visitor', timestamp_default=False
):
    """
    Creates the subject line for the contact email using the config or the provided
    subject.

    :param subject: a user defined subject line
    :param default: the default str to use if the user didn't provide a subject or
        ckanext.contact.subject isn't specified
    :param timestamp_default: the default bool to use if add_timestamp_to_subject isn't
        specified
    :returns: the subject line
    """
    if not subject:
        subject = toolkit.config.get('ckanext.contact.subject', toolkit._(default))
    if asbool(
        toolkit.config.get(
            'ckanext.contact.add_timestamp_to_subject', timestamp_default
        )
    ):
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
        subject = f'{subject} [{timestamp}]'

    prefix = toolkit.config.get('ckanext.contact.subject_prefix', '')

    return f'{prefix}{" " if prefix else ""}{subject}'
from flask import jsonify
import ckan.model as model   # 👈 import model explicitly

def submit():
    """
    Take the data in the request params and send an email using them. If the data is
    invalid or a recaptcha is setup and it fails, don't send the email.

    :returns: a dict of details
    """
    # Build CKAN context
    context = {
        'model': model,
        'session': model.Session,
        'user': toolkit.g.user,  # current CKAN username (string)
        'auth_user_obj': toolkit.g.userobj,  # full User object
    }

    # this variable holds the status of sending the email
    email_success = True

    # pull out the data from the request
    data_dict = logic.clean_dict(
        unflatten(logic.tuplize_dict(logic.parse_params(toolkit.request.values)))
    )

    # validate the request params
    errors, error_summary, recaptcha_error = validate(data_dict)

    # if there are not errors and no recaptcha error, attempt to send the email
    if len(errors) == 0 and recaptcha_error is None:
        body_parts = [
            f'{data_dict["content"]}\n',
            'Στοιχεία αποστολέα:',
            f'  Όνομα: {data_dict["name"]}',
            f'  Email: {data_dict["email"]}',
        ]

        # Προσθήκη επιπλέον δομημένων πεδίων για καλύτερο πλαίσιο
        actor_type = data_dict.get('actor_type')
        if actor_type:
            body_parts.append(f'  Τύπος αποστολέα: {actor_type}')

        organization = data_dict.get('organization')
        if organization:
            org_label = organization
            org_url = None
            try:
                org_dict = toolkit.get_action('organization_show')(context, {'id': organization})
                org_name = org_dict.get('title') or org_dict.get('name') or organization
                org_label = org_name
                # Προσθήκη συνδέσμου προς τη σελίδα του οργανισμού, αν είναι εφικτό
                try:
                    site_url = config.get('ckan.site_url', '').rstrip('/')
                    org_name_for_url = org_dict.get('name')
                    if site_url and org_name_for_url:
                        org_url = f'{site_url}/organization/{org_name_for_url}'
                except Exception:
                    org_url = None
            except Exception as e:
                log.error(f'Error resolving organization {organization} in contact email: {e}')

            if org_url:
                body_parts.append(f'  Οργανισμός: {org_label} ({org_url})')
            else:
                body_parts.append(f'  Οργανισμός: {org_label}')

        role = data_dict.get('role')
        if role:
            body_parts.append(f'  Ρόλος: {role}')

        # Ανθρώπινα labels για τις βασικές κατηγορίες θέματος
        subject_type_labels = {
            'general_question': 'Γενική ερώτηση',
            'open_new_data': 'Αίτημα για δεδομένα',
            'api_or_technical': 'Αναφορά σφάλματος',
            'general_feedback': 'Γενικό σχόλιο / βελτίωση',
            'account': 'Πρόβλημα με λογαριασμό / σύνδεση',
            'dataset_publication': 'Δημοσίευση / ενημέρωση συνόλου δεδομένων',
            'other': 'Άλλο',
        }

        support_choice_labels = {
            'specific_data': 'Συγκεκριμένα δεδομένα ή σύνολο δεδομένων',
            'procedure': 'Μια διοικητική διαδικασία',
            'portal_usage': 'Τη χρήση του data.gov.gr',
            'new_datasets': 'Νέα σύνολα δεδομένων που δεν βρίσκετε',
            'update_existing': 'Ενημέρωση ή διόρθωση υπαρχόντων δεδομένων',
            'licensing': 'Διευκρινίσεις για άδειες και όρους χρήσης',
            'page_error': 'Σελίδα που εμφανίζει σφάλμα',
            'api_issue': 'API που δεν ανταποκρίνεται όπως αναμένεται',
            'data_inconsistency': 'Λανθασμένα ή ασυνεπή δεδομένα',
            'feedback_usability': 'Εμπειρία χρήσης / ευχρηστία της πύλης',
            'feedback_features': 'Προτάσεις για νέες λειτουργίες',
            'feedback_content': 'Σχόλιο ή παρατήρηση για το περιεχόμενο',
        }

        subject_type = data_dict.get('subject_type')
        if subject_type:
            label = subject_type_labels.get(subject_type, subject_type)
            body_parts.append(f'  Κατηγορία θέματος: {label}')

        related_reference = data_dict.get('related_reference')
        if related_reference:
            body_parts.append(f'  Related dataset/service: {related_reference}')

        support_scope = data_dict.get('support_question_scope')
        if support_scope:
            scope_label = support_choice_labels.get(support_scope, support_scope)
            body_parts.append(f'  Υποκατηγορία αιτήματος: {scope_label}')


        # -------------- Ανάκτηση στοιχείων Υπεύθυνου Επικοινωνίας από ini------
        # Email Αποστολέα ορίζεται αυτόματα από smtp.mail_from = avadrachanis@ots.gr
        # Email Παραλήπτη ορίζεται από ckanext.contact.mail_to = ckardamanidis@ots.gr

        recipient_email = toolkit.config.get('ckanext.contact.mail_to') or toolkit.config.get('email_to')
        # Όνομα Παραλήπτη ορίζεται από ckanext.contact.recipient_name Data.gov.gr
        recipient_name = toolkit.config.get('ckanext.contact.recipient_name') or toolkit.config.get('ckan.site_title')

        author_email = None
        org_email = None

        # --------------Ανάκτηση Στοιχείων Συντάκτη--------
        if "package_id" in data_dict:

            # Ανάκτηση συνόλου δεδομένων
            pkg_dict = toolkit.get_action('package_show')(context, {'id': data_dict["package_id"]})

            site_url = config.get('ckan.site_url')
            dataset_url = f'{site_url}/dataset/{pkg_dict["name"]}'

            # Στοιχεία Συντάκτη
            author_email = pkg_dict.get("publisher")[0].get('email')

            body_parts.append(f'  Dataset URL : {dataset_url}')

            # --------------Ανάκτηση email Οργανισμού--------
            org_summary = pkg_dict.get('organization')
            if not org_summary:
                return jsonify({'error': 'Package has no organization'}), 404

            org_id = org_summary['id']
            org_dict = toolkit.get_action('organization_show')(context, {'id': org_id})
            org_email = org_dict.get('email')


        # Δημιουργία Λίστας από παραλήπτες = Υπεύθυνος Επικοινωνίας + Συντάκτης + Λίστα από Διαχειριστών οργανισμού συνόλου δεδομένων
        recipients = []

        if recipient_email:
            recipients.append(recipient_email)

        if author_email:
            recipients.append(author_email)

        if org_email:
            recipients.append(org_email)

        # Ανάκτηση στοιχείων χρήστη
        user, fullname, email = get_current_user_info(context)

        mail_dict = {
            'recipient_email': recipients,
            'recipient_name': recipient_name,
            # κρατάμε τη συμπεριφορά του extension: είτε το subject που πληκτρολόγησε
            # ο χρήστης, είτε το default από build_subject
            'subject': build_subject(subject=data_dict.get('subject')),
            'body': '\n'.join(body_parts),
            'headers': {'reply-to': email},
        }

        # allow other plugins to modify the mail_dict
        for plugin in PluginImplementations(IContact):
            plugin.mail_alter(mail_dict, data_dict)

        # note the pop here so that we don't get parameter clashes when we call
        # mail_recipient below
        emails = mail_dict.pop('recipient_email')
        names = mail_dict.pop('recipient_name')
        if isinstance(emails, str):
            emails = [emails]
            names = [names]

        # send the email to each name/email pair
        for name, email in zip(names, emails):
            try:
                mailer.mail_recipient(name, email, **mail_dict)
            except (mailer.MailerException, socket.error):
                email_success = False

    return {
        'success': recaptcha_error is None and len(errors) == 0 and email_success,
        'data': data_dict,
        'errors': errors,
        'error_summary': error_summary,
        'recaptcha_error': recaptcha_error,
    }

def get_current_user_info(context):
    """
    Returns (username, fullname, email) of the current CKAN user
    from the context dict.
    """
    user_name = context.get('user')
    if not user_name:
        return None, None, None

    # Fetch the user object from CKAN model
    user_obj = model.User.get(user_name)

    if not user_obj:
        return None, None, None

    return user_obj.name, user_obj.fullname, user_obj.email
