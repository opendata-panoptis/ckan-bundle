import logging

import ckan.lib.uploader as uploader
import ckan.plugins.toolkit as toolkit


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

    # Κάνουμε skip τους ελέγχους για την αποθήκευση των πακέτων
    context['ignore_auth'] = True
    pkg = toolkit.get_action('package_update')(context, data_dict)

    return pkg
