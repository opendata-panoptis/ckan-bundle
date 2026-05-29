# !/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-contact
# Created by the Natural History Museum in London, UK
import logging
import mimetypes
import re
import smtplib
import socket
from datetime import datetime, timezone
from email import utils
from email.message import EmailMessage
from time import time

import ckan
import ckan.model as model
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
            invalid_email_message = toolkit._('Email address appears to be invalid')
            errors['email'] = [invalid_email_message]
            error_summary['email'] = invalid_email_message

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


def _dedupe_emails(emails):
    unique_emails = []
    seen_emails = set()

    for email in emails:
        normalized_email = (email or '').strip()
        if not normalized_email or normalized_email in seen_emails:
            continue
        unique_emails.append(normalized_email)
        seen_emails.add(normalized_email)

    return unique_emails


def parse_recipient_emails(value):
    if value is None:
        return []

    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]

    emails = []
    for item in values:
        if item is None:
            continue
        if not isinstance(item, str):
            item = str(item)
        emails.extend(re.split(r'[\n,]+', item))

    return _dedupe_emails(emails)


def get_central_recipient_emails():
    recipient_value = toolkit.config.get('ckanext.contact.mail_to') or toolkit.config.get(
        'email_to'
    )
    return parse_recipient_emails(recipient_value)


def get_dataset_recipient_emails(context, data_dict):
    if not data_dict.get('package_id'):
        return None, []

    pkg_dict = toolkit.get_action('package_show')(
        context, {'id': data_dict['package_id']}
    )

    dataset_url = None
    site_url = (config.get('ckan.site_url') or '').rstrip('/')
    if site_url and pkg_dict.get('name'):
        dataset_url = f'{site_url}/dataset/{pkg_dict["name"]}'

    recipients = []

    send_to_author = asbool(
        toolkit.config.get('ckanext.contact.send_to_author_email', False)
    )
    publishers = pkg_dict.get('publisher') or []
    if send_to_author and publishers and isinstance(publishers[0], dict):
        recipients.append(publishers[0].get('email'))

    org_summary = pkg_dict.get('organization')
    if org_summary and org_summary.get('id'):
        org_dict = toolkit.get_action('organization_show')(
            context, {'id': org_summary['id']}
        )
        recipients.append(org_dict.get('email'))

    return dataset_url, _dedupe_emails(recipients)


def _format_recipient_header(emails, recipient_name=None):
    if not emails:
        return None
    if len(emails) == 1 and recipient_name:
        return utils.formataddr((recipient_name, emails[0]))
    return ', '.join(emails)


def send_grouped_email(
    to_emails,
    cc_emails,
    subject,
    body,
    recipient_name=None,
    body_html=None,
    headers=None,
    attachments=None,
):
    to_emails = parse_recipient_emails(to_emails)
    cc_emails = [
        email for email in parse_recipient_emails(cc_emails) if email not in to_emails
    ]

    if not to_emails and cc_emails:
        to_emails = cc_emails[:1]
        cc_emails = cc_emails[1:]

    envelope_recipients = _dedupe_emails(to_emails + cc_emails)
    if not envelope_recipients:
        raise mailer.MailerException('No recipient email address available!')

    if not headers:
        headers = {}

    if not attachments:
        attachments = []

    mail_from = config.get('smtp.mail_from')
    reply_to = config.get('smtp.reply_to')

    msg = EmailMessage()
    msg.set_content(body, cte='base64')

    if body_html:
        msg.add_alternative(body_html, subtype='html', cte='base64')

    for k, v in headers.items():
        if k in msg.keys():
            msg.replace_header(k, v)
        else:
            msg.add_header(k, v)

    msg['Subject'] = subject
    msg['From'] = utils.formataddr((config.get('ckan.site_title') or '', mail_from))

    to_header = _format_recipient_header(to_emails, recipient_name=recipient_name)
    if to_header:
        msg['To'] = to_header

    cc_header = _format_recipient_header(cc_emails)
    if cc_header:
        msg['Cc'] = cc_header

    msg['Date'] = utils.formatdate(time())
    if not config.get('ckan.hide_version'):
        msg['X-Mailer'] = f'CKAN {ckan.__version__}'

    if reply_to and reply_to != '' and not msg['Reply-to']:
        msg['Reply-to'] = reply_to

    for attachment in attachments:
        if len(attachment) == 3:
            name, _file, media_type = attachment
        else:
            name, _file = attachment
            media_type = None

        if not media_type:
            media_type, _encoding = mimetypes.guess_type(name)
        if media_type:
            main_type, sub_type = media_type.split('/')
        else:
            main_type = sub_type = None

        msg.add_attachment(
            _file.read(), filename=name, maintype=main_type, subtype=sub_type
        )

    smtp_server = config.get('smtp.server')
    smtp_starttls = config.get('smtp.starttls')
    smtp_user = config.get('smtp.user')
    smtp_password = config.get('smtp.password')

    try:
        smtp_connection = smtplib.SMTP(smtp_server)
    except (socket.error, smtplib.SMTPConnectError) as e:
        log.exception(e)
        raise mailer.MailerException(
            'SMTP server could not be connected to: "%s" %s' % (smtp_server, e)
        )

    try:
        smtp_connection.ehlo()

        if smtp_starttls:
            if smtp_connection.has_extn('STARTTLS'):
                smtp_connection.starttls()
                smtp_connection.ehlo()
            else:
                raise mailer.MailerException('SMTP server does not support STARTTLS')

        if smtp_user:
            assert smtp_password, (
                'If smtp.user is configured then '
                'smtp.password must be configured as well.'
            )
            smtp_connection.login(smtp_user, smtp_password)

        refused_recipients = smtp_connection.sendmail(
            mail_from, envelope_recipients, msg.as_string()
        )
        if refused_recipients:
            refused_addresses = set(refused_recipients.keys())
            accepted_addresses = [
                email for email in envelope_recipients if email not in refused_addresses
            ]
            log.warning(
                'Grouped email accepted for %s and refused for %s',
                ', '.join(accepted_addresses) or 'no recipients',
                ', '.join(sorted(refused_addresses)),
            )
        else:
            log.info('Sent grouped email to %s', ', '.join(envelope_recipients))
    except smtplib.SMTPException as e:
        error_message = '%r' % e
        log.exception(error_message)
        raise mailer.MailerException(error_message)
    finally:
        smtp_connection.quit()


def _pop_grouped_recipient_fields(mail_dict):
    to_emails = parse_recipient_emails(mail_dict.pop('to_emails', []))
    cc_emails = parse_recipient_emails(mail_dict.pop('cc_emails', []))
    legacy_emails = parse_recipient_emails(mail_dict.pop('recipient_email', []))
    default_legacy_emails = parse_recipient_emails(
        mail_dict.pop('_default_recipient_email', [])
    )
    recipient_name = mail_dict.pop('recipient_name', None)

    legacy_override = legacy_emails != default_legacy_emails

    if legacy_override:
        # Preserve the long-standing mail_alter contract: replacing
        # recipient_email overrides the default recipients entirely.
        to_emails = legacy_emails
        cc_emails = []
    elif not to_emails and not cc_emails:
        to_emails = legacy_emails
    elif legacy_emails:
        known_recipients = set(to_emails + cc_emails)
        to_emails.extend(
            email for email in legacy_emails if email not in known_recipients
        )

    cc_emails = [email for email in cc_emails if email not in to_emails]

    if not to_emails and cc_emails:
        to_emails = cc_emails[:1]
        cc_emails = cc_emails[1:]

    return recipient_name, to_emails, cc_emails


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
            'open_new_data': 'Αίτημα διάθεσης νέου ανοικτού συνόλου δεδομένων',
            'api_or_technical': 'Τεχνικό πρόβλημα-σφάλμα με λογαριασμό/Εθνική Πύλη',
            'general_feedback': 'Πρόταση βελτίωσης της Εθνικής Πύλης',
            'account': 'Πρόβλημα με λογαριασμό / σύνδεση',
            'dataset_publication': 'Δημοσίευση / ενημέρωση συνόλου δεδομένων',
            'other': 'Άλλο',
        }

        support_choice_labels = {
            'specific_data': 'Συγκεκριμένα δεδομένα ή σύνολο δεδομένων',
            'procedure': 'Μια διοικητική διαδικασία',
            'portal_usage': 'Τη χρήση του data.gov.gr',
            'account_issue': 'Πρόβλημα με λογαριασμό ή σύνδεση',
            'portal_error': 'Σφάλμα ή δυσλειτουργία στην Εθνική Πύλη',
            'service_issue': 'Τεχνικό πρόβλημα σε υπηρεσία ή σελίδα',
            'new_datasets': 'Νέα σύνολα δεδομένων που δεν βρίσκετε',
            'update_existing': 'Ενημέρωση ή διόρθωση υπαρχόντων δεδομένων',
            'agency_data_request': 'Αίτημα διάθεσης δεδομένων από φορέα',
            'feedback_usability': 'Βελτίωση ευχρηστίας ή πλοήγησης',
            'feedback_features': 'Πρόταση για νέα λειτουργία',
            'feedback_content': 'Βελτίωση περιεχομένου ή πληροφόρησης',
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


        # Email παραληπτών από ckanext.contact.mail_to / email_to.
        central_recipient_emails = get_central_recipient_emails()
        # Όνομα παραλήπτη χρησιμοποιείται μόνο όταν υπάρχει ένα κεντρικό To recipient.
        recipient_name = toolkit.config.get('ckanext.contact.recipient_name') or toolkit.config.get('ckan.site_title')

        # --------------Ανάκτηση Στοιχείων Συντάκτη--------
        dataset_url, dataset_recipient_emails = get_dataset_recipient_emails(
            context, data_dict
        )
        if dataset_url:
            body_parts.append(f'  Dataset URL : {dataset_url}')

        if dataset_recipient_emails:
            to_emails = dataset_recipient_emails
            cc_emails = central_recipient_emails
            to_recipient_name = None
        else:
            to_emails = central_recipient_emails
            cc_emails = []
            to_recipient_name = recipient_name

        mail_dict = {
            'recipient_email': _dedupe_emails(to_emails + cc_emails),
            '_default_recipient_email': _dedupe_emails(to_emails + cc_emails),
            'recipient_name': to_recipient_name,
            'to_emails': to_emails,
            'cc_emails': cc_emails,
            # κρατάμε τη συμπεριφορά του extension: είτε το subject που πληκτρολόγησε
            # ο χρήστης, είτε το default από build_subject
            'subject': build_subject(subject=data_dict.get('subject')),
            'body': '\n'.join(body_parts),
            'headers': {'reply-to': data_dict.get('email')},
        }

        # allow other plugins to modify the mail_dict
        for plugin in PluginImplementations(IContact):
            plugin.mail_alter(mail_dict, data_dict)

        recipient_name, to_emails, cc_emails = _pop_grouped_recipient_fields(
            mail_dict
        )

        try:
            send_grouped_email(
                to_emails=to_emails,
                cc_emails=cc_emails,
                recipient_name=recipient_name,
                **mail_dict,
            )
        except (mailer.MailerException, socket.error):
            email_success = False

    return {
        'success': recaptcha_error is None and len(errors) == 0 and email_success,
        'data': data_dict,
        'errors': errors,
        'error_summary': error_summary,
        'recaptcha_error': recaptcha_error,
    }
