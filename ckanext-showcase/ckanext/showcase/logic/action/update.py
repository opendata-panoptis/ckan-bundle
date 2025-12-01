import logging

import ckan.lib.uploader as uploader
import ckan.plugins.toolkit as toolkit
from ckan.lib.mailer import mail_recipient

from ckanext.showcase.views import send_approved_showcase_email

log = logging.getLogger(__name__)


def showcase_update(context, data_dict):
    # Ανάκτηση του υφιστάμενου showcase για να πάρουμε το πραγματικό παλιό image_url
    try:
        existing_pkg = toolkit.get_action('package_show')(context, {'id': data_dict['id']})
        old_image_url = existing_pkg.get('image_url', '')
    except Exception:
        # Αν αποτύχει η ανάκτηση, δεν περνάμε παλιό filename
        old_image_url = ''

    # Περνάμε το πραγματικό παλιό image_url ως old_filename
    # Μόνο αν δεν είναι κενό και δεν είναι HTTP URL
    old_filename = None
    if (old_image_url and
            old_image_url.strip() and
            not old_image_url.startswith('http')):
        old_filename = old_image_url

    upload = uploader.get_uploader('showcase', old_filename)

    upload.update_data_dict(data_dict, 'image_url',
                            'image_upload', 'clear_upload')

    upload.upload(uploader.get_max_image_size())

    # TODO: Να ελεγχθεί αν έχει side effects

    # Αν δεν υπάρχει το πεδίο approval_status,
    #   που σημαίνει ότι γίνεται update από φόρμα που δεν στέλνει το πεδίο,
    #   δηλαδή από κάποιον πέραν του διαχειριστή, από τον δημιουργό του showcase, τον πολίτη,
    #   ορίζουμε την προεπιλεγμένη τιμή
    if 'approval_status' not in data_dict:
        data_dict['approval_status'] = 'pending'
    elif ('approval_status' in data_dict) and (data_dict['approval_status'] == 'approved'):

        #result = toolkit.get_action('showcase_package_list')(context, data_dict)
        packages = toolkit.get_action('ckanext_showcase_package_list')(context, {'showcase_id': data_dict['id']})
        #org_ids = [pkg['organization']['id'] for pkg in packages]

        #organizations = [
        #    toolkit.get_action('organization_show')(context, {'id': oid})
        #    for oid in org_ids
        #]

        for pkg in packages:

            org_id = pkg['organization']['id']

            org = toolkit.get_action('organization_show')(context, {'id': org_id})

            receive_dataset_showcase_emails = org.get('receive_dataset_email_updates', '')

            if receive_dataset_showcase_emails == True:
                org_email = org.get('email', '')

                from ckan.common import config
                site_url = config.get('ckan.site_url', 'http://localhost:5000')
                showcase_url = f"{site_url}/showcase/{pkg['name']}"

                dataset_name = pkg.get('title', '')
                send_approved_showcase_dataset_email(context, org_email, data_dict, showcase_url, dataset_name)

    # Κάνουμε skip τους ελέγχους για την αποθήκευση των πακέτων
    context['ignore_auth'] = True
    pkg = toolkit.get_action('package_update')(context, data_dict)

    return pkg

def send_approved_showcase_dataset_email(context, recipient, data_dict, showcase_url, dataset_name):
    mail_recipient(
            recipient_name="",
            recipient_email=recipient,
            subject=f"DATA GOV GR: Σύνολο Δεδομένων σε εγκεκριμένη εφαρμογή: '{dataset_name}'",
            body=f"Η Εφαρμογή '{data_dict['title']}' εγκρίθηκε για το Σύνολο Δεδομένων '{dataset_name}'. Η εφαρμογή είναι διαθέσιμη εδώ. URL: '{showcase_url}'"
    )