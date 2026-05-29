import logging

import ckan.model as model
import ckan.plugins.toolkit as toolkit
from ckan.lib.mailer import mail_recipient
from sqlalchemy import text

from ckanext.showcase.logic.auth import _is_showcase_admin

log = logging.getLogger(__name__)

RECIPIENTS_MODE_CONFIG = 'ckanext.showcase.notification_recipients_mode'
CONFIGURED_EMAIL_CONFIG = 'ckanext.showcase.notification_email'
EXCLUDED_EMAILS_CONFIG = 'ckanext.showcase.notification_exclude_emails'

RECIPIENTS_MODE_ALL_ADMINS = 'all_admins'
RECIPIENTS_MODE_SHOWCASE_ADMINS = 'showcase_admins'
RECIPIENTS_MODE_CONFIGURED_ONLY = 'configured_email_only'

VALID_RECIPIENT_MODES = {
    RECIPIENTS_MODE_ALL_ADMINS,
    RECIPIENTS_MODE_SHOWCASE_ADMINS,
    RECIPIENTS_MODE_CONFIGURED_ONLY,
}


def normalize_email(email):
    if not email:
        return None

    email = email.strip().lower()
    return email or None


def get_email_from_id(context, user_id):
    try:
        row = model.Session.execute(
            text("""
                SELECT lower(trim(email)) AS email
                FROM "user"
                WHERE id = :user_id
                  AND state = 'active'
                  AND email IS NOT NULL
            """),
            {'user_id': user_id}
        ).fetchone()
    except Exception:
        return None

    return row[0] if row and row[0] else None


def _parse_email_list(raw_value):
    if not raw_value:
        return []

    raw_value = raw_value.replace(';', ',').replace('\n', ',')
    emails = []

    for item in raw_value.split(','):
        email = normalize_email(item)
        if email:
            emails.append(email)

    return emails


def _get_notification_mode():
    mode = toolkit.config.get(
        RECIPIENTS_MODE_CONFIG,
        RECIPIENTS_MODE_ALL_ADMINS
    )
    mode = (mode or '').strip().lower()

    if mode not in VALID_RECIPIENT_MODES:
        log.warning(
            'Invalid value "%s" for %s. Falling back to "%s".',
            mode,
            RECIPIENTS_MODE_CONFIG,
            RECIPIENTS_MODE_ALL_ADMINS
        )
        return RECIPIENTS_MODE_ALL_ADMINS

    return mode


def _get_configured_notification_email():
    return normalize_email(toolkit.config.get(CONFIGURED_EMAIL_CONFIG))


def _get_excluded_notification_emails():
    return set(_parse_email_list(toolkit.config.get(EXCLUDED_EMAILS_CONFIG, '')))


def _get_admin_emails(include_sysadmins=True, include_showcase_admins=True):
    admin_emails = set()
    conditions = []

    if include_sysadmins:
        conditions.append('u.sysadmin = true')

    if include_showcase_admins:
        conditions.append('sa.user_id IS NOT NULL')

    if not conditions:
        return admin_emails

    query = text("""
        SELECT DISTINCT lower(trim(u.email)) AS email
        FROM "user" u
        LEFT JOIN showcase_admin sa ON sa.user_id = u.id
        WHERE u.state = 'active'
          AND u.email IS NOT NULL
          AND ({conditions})
    """.format(conditions=' OR '.join(conditions)))

    rows = model.Session.execute(query).fetchall()

    for row in rows:
        if row[0]:
            admin_emails.add(row[0])

    return admin_emails


def get_showcase_notification_emails():
    recipients = set()
    mode = _get_notification_mode()
    configured_email = _get_configured_notification_email()
    excluded_emails = _get_excluded_notification_emails()

    if mode == RECIPIENTS_MODE_ALL_ADMINS:
        recipients.update(
            _get_admin_emails(
                include_sysadmins=True,
                include_showcase_admins=True,
            )
        )
    elif mode == RECIPIENTS_MODE_SHOWCASE_ADMINS:
        recipients.update(
            _get_admin_emails(
                include_sysadmins=False,
                include_showcase_admins=True,
            )
        )
    elif mode == RECIPIENTS_MODE_CONFIGURED_ONLY:
        if configured_email:
            recipients.add(configured_email)
        else:
            log.warning(
                'Notification mode is "%s" but no configured email was found in %s.',
                RECIPIENTS_MODE_CONFIGURED_ONLY,
                CONFIGURED_EMAIL_CONFIG,
            )

    if mode != RECIPIENTS_MODE_CONFIGURED_ONLY and configured_email:
        recipients.add(configured_email)

    return [
        email for email in recipients
        if email not in excluded_emails
    ]


def _get_showcase_title(showcase):
    return (
        showcase.get('title')
        or showcase.get('name')
        or ''
    )


def _get_showcase_url(showcase):
    site_url = toolkit.config.get('ckan.site_url', 'http://localhost:5000')
    showcase_name = showcase.get('name') or ''
    return f"{site_url}/showcase/{showcase_name}"


def _get_notification_subject_prefix():
    return (toolkit.config.get('ckan.site_title', '') or '').strip()


def _format_notification_subject(subject):
    prefix = _get_notification_subject_prefix()
    if prefix:
        return f"{prefix}: {subject}"
    return subject


def _send_email(recipient, subject, body):
    mail_recipient(
        recipient_name="",
        recipient_email=recipient,
        subject=subject,
        body=body,
    )


def _send_email_safely(recipient, subject, body, *, event, audience):
    try:
        _send_email(recipient, subject, body)
        return True
    except Exception:
        log.exception(
            "Failed to send showcase %s email to %s recipient %s",
            event,
            audience,
            recipient,
        )
        return False


def _get_creator_creation_subject(showcase_title):
    return _format_notification_subject(
        f"Δημιουργήθηκε η επανάχρησή σας: '{showcase_title}'"
    )


def _get_creator_creation_body(showcase_title, showcase_url, status):
    if status == 'approved':
        return (
            f"Η επανάχρησή σας '{showcase_title}' δημιουργήθηκε επιτυχώς "
            f"και είναι διαθέσιμη εδώ: '{showcase_url}'"
        )
    if status == 'rejected':
        return (
            f"Η επανάχρησή σας '{showcase_title}' δημιουργήθηκε, αλλά η "
            f"τρέχουσα κατάστασή της είναι απορριφθείσα. Μπορείτε να την "
            f"επισκεφθείτε εδώ: '{showcase_url}'"
        )
    return (
        f"Η επανάχρησή σας '{showcase_title}' υποβλήθηκε επιτυχώς και "
        f"βρίσκεται σε αναμονή ελέγχου. Μπορείτε να την επισκεφθείτε "
        f"εδώ: '{showcase_url}'"
    )


def _get_admin_creation_subject(showcase_title):
    return _format_notification_subject(
        f"Δημιουργήθηκε νέα επανάχρηση: '{showcase_title}'"
    )


def _get_admin_creation_body(showcase_title, showcase_url, status):
    if status == 'approved':
        return (
            f"Δημιουργήθηκε νέα επανάχρηση με τίτλο '{showcase_title}' και "
            f"η τρέχουσα κατάστασή της είναι εγκεκριμένη. Μπορείτε να την "
            f"προβάλετε εδώ: '{showcase_url}'"
        )
    if status == 'rejected':
        return (
            f"Δημιουργήθηκε νέα επανάχρηση με τίτλο '{showcase_title}' και "
            f"η τρέχουσα κατάστασή της είναι απορριφθείσα. Μπορείτε να την "
            f"προβάλετε εδώ: '{showcase_url}'"
        )
    return (
        f"Δημιουργήθηκε νέα επανάχρηση με τίτλο '{showcase_title}' και "
        f"βρίσκεται σε αναμονή ελέγχου. Μπορείτε να την ελέγξετε "
        f"εδώ: '{showcase_url}'"
    )


def _get_creator_status_subject(showcase_title, status):
    if status == 'approved':
        return _format_notification_subject(
            f"Εγκρίθηκε η επανάχρησή σας: '{showcase_title}'"
        )
    return _format_notification_subject(
        f"Απορρίφθηκε η επανάχρησή σας: '{showcase_title}'"
    )


def _get_creator_status_body(showcase_title, showcase_url, status):
    if status == 'approved':
        return (
            f"Η επανάχρησή σας '{showcase_title}' εγκρίθηκε και είναι "
            f"διαθέσιμη εδώ: '{showcase_url}'"
        )
    return (
        f"Η επανάχρησή σας '{showcase_title}' απορρίφθηκε. Μπορείτε να τη "
        f"δείτε εδώ: '{showcase_url}'"
    )


def _get_org_approval_subject(dataset_title):
    return _format_notification_subject(
        "Εγκεκριμένη επανάχρηση για σύνολο δεδομένων σας: "
        f"'{dataset_title}'"
    )


def _get_org_approval_body(showcase_title, dataset_title, showcase_url):
    return (
        f"Η επανάχρηση '{showcase_title}' που αξιοποιεί το σύνολο δεδομένων "
        f"'{dataset_title}' εγκρίθηκε. Μπορείτε να τη δείτε εδώ: '{showcase_url}'"
    )


def send_showcase_created_notifications(context, showcase, requested_status=None):
    showcase_title = _get_showcase_title(showcase)
    showcase_url = _get_showcase_url(showcase)
    status = showcase.get('approval_status') or requested_status or 'pending'
    creator_email = normalize_email(
        get_email_from_id(context, showcase.get('creator_user_id'))
    )
    admin_emails = [
        email for email in (
            normalize_email(email) for email in get_showcase_notification_emails()
        )
        if email and email != creator_email
    ]

    if creator_email:
        _send_email_safely(
            creator_email,
            _get_creator_creation_subject(showcase_title),
            _get_creator_creation_body(showcase_title, showcase_url, status),
            event='created',
            audience='creator',
        )

    sent = {creator_email} if creator_email else set()
    for admin_email in admin_emails:
        if admin_email in sent:
            continue
        _send_email_safely(
            admin_email,
            _get_admin_creation_subject(showcase_title),
            _get_admin_creation_body(showcase_title, showcase_url, status),
            event='created',
            audience='admin',
        )
        sent.add(admin_email)


def _get_approved_dataset_org_recipients(context, showcase):
    recipients = []
    packages = toolkit.get_action('ckanext_showcase_package_list')(
        context,
        {
            'showcase_id': showcase['id'],
            'package_type': 'dataset',
        },
    )

    for package in packages:
        org = package.get('organization') or {}
        org_id = org.get('id')
        if not org_id:
            continue

        org_dict = toolkit.get_action('organization_show')(context, {'id': org_id})
        receive_dataset_showcase_emails = org_dict.get(
            'receive_dataset_email_updates',
            '',
        )

        if receive_dataset_showcase_emails is not True:
            continue

        org_email = normalize_email(org_dict.get('email'))
        if not org_email:
            log.warning(
                "Skipping showcase approval email for organization %s: empty email",
                org_id,
            )
            continue

        recipients.append({
            'email': org_email,
            'dataset_title': package.get('title') or package.get('name') or '',
        })

    return recipients


def send_showcase_status_change_notifications(
    context,
    previous_showcase,
    updated_showcase,
    requested_status=None,
):
    current_status = previous_showcase.get('approval_status', '')
    new_status = updated_showcase.get('approval_status') or requested_status or ''

    if current_status == new_status:
        return
    if new_status not in ('approved', 'rejected'):
        return
    if not _is_showcase_admin(context):
        return

    showcase_title = _get_showcase_title(updated_showcase) or _get_showcase_title(
        previous_showcase
    )
    showcase_name = updated_showcase.get('name') or previous_showcase.get('name')
    showcase_id = updated_showcase.get('id') or previous_showcase.get('id')
    showcase_url = _get_showcase_url({'name': showcase_name})
    creator_email = normalize_email(
        get_email_from_id(context, previous_showcase.get('creator_user_id'))
    )

    if creator_email:
        _send_email_safely(
            creator_email,
            _get_creator_status_subject(showcase_title, new_status),
            _get_creator_status_body(showcase_title, showcase_url, new_status),
            event=new_status,
            audience='creator',
        )

    if new_status != 'approved':
        return

    showcase_ref = {
        'id': showcase_id,
        'name': showcase_name,
        'title': showcase_title,
    }
    for recipient in _get_approved_dataset_org_recipients(context, showcase_ref):
        if recipient['email'] == creator_email:
            continue
        _send_email_safely(
            recipient['email'],
            _get_org_approval_subject(recipient['dataset_title']),
            _get_org_approval_body(
                showcase_title,
                recipient['dataset_title'],
                showcase_url,
            ),
            event=new_status,
            audience='organization',
        )
