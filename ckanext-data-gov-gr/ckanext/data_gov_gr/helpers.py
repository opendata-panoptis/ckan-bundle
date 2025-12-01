import logging
import ckan.plugins.toolkit as toolkit
from ckan.lib import helpers as core_helpers
from ckan.lib.helpers import lang
from ckan.plugins.toolkit import _ # Import για το σύστημα μετάφρασης

log = logging.getLogger(__name__)

# Αποθηκεύουμε τα δεδομένα από τη βάση σε ένα cache για καλύτερη απόδοση
_vocabulary_cache = {}

def _get_vocabulary_tags(vocabulary_id_or_name):
    """
    Ανακτά τα tags ενός λεξιλογίου από τη βάση δεδομένων.
    Χρησιμοποιεί cache για καλύτερη απόδοση.
    """
    if vocabulary_id_or_name in _vocabulary_cache:
        return _vocabulary_cache[vocabulary_id_or_name]

    try:
        vocabulary_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
            {}, {'id': vocabulary_id_or_name}
        )
        tags = vocabulary_data.get('tags', [])

        # Αποθήκευση στο cache
        _vocabulary_cache[vocabulary_id_or_name] = tags

        return tags
    except toolkit.ObjectNotFound:
        log.warning(f'Vocabulary not found: "{vocabulary_id_or_name}"')
        return []
    except Exception as e:
        log.exception(f'Error retrieving vocabulary "{vocabulary_id_or_name}": {e}')
        return []

def _get_label_by_language(tag):
    current_lang = lang()

    if current_lang == 'el':
        return tag.get('label_el') or tag.get('label_en') or tag.get('display_name')
    elif current_lang == 'en':
        return tag.get('label_en') or tag.get('display_name')

    return tag.get('display_name')

def vocabulary_facet_item_label(name):
    """
    Μέθοδος για αλλαγή του label ενός facet item.
    Ανακτά τα δεδομένα από τη βάση αντί για hardcoded τιμές.
    """
    lang_system = lang()
    display_name = name['display_name']

    # Μεταφράζουμε τις boolean τιμές που το CKAN εμφανίζει ως 'Yes'/'No'
    # Οι πραγματικές τιμές στο Solr είναι 'true' και 'false'.
    # Ο έλεγχος γίνεται case-insensitive για ασφάλεια.
    if str(display_name).lower() == 'true' or str(display_name).lower() == 'yes':
        return _('Yes') if lang_system == 'en' else _('Ναι')

    if str(display_name).lower() == 'false' or str(display_name).lower() == 'no':
        return _('No') if lang_system == 'en' else _('Όχι')

    if display_name.startswith('http://purl.org/adms/publishertype/'):
        code = display_name.split('/')[-1]
        tags = _get_vocabulary_tags('Publisher type')
        for tag in tags:
            if tag.get('value_uri') == display_name or tag.get('name') == code:
                return _get_label_by_language(tag) or code
        return code

    # Έλεγχος για το λεξιλόγιο Access right
    if display_name.startswith('http://publications.europa.eu/resource/authority/access-right/'):
        access_code = display_name.split('/')[-1]
        tags = _get_vocabulary_tags('Access right')

        for tag in tags:
            if tag.get('value_uri') == display_name or tag.get('name') == access_code:
                return _get_label_by_language(tag) or access_code

        return access_code

    # Έλεγχος για το λεξιλόγιο Planned availability
    if display_name.startswith('http://publications.europa.eu/resource/authority/planned-availability/'):
        availability_code = display_name.split('/')[-1]
        tags = _get_vocabulary_tags('Planned availability')

        for tag in tags:
            if tag.get('value_uri') == display_name or tag.get('name') == availability_code:
                return _get_label_by_language(tag) or availability_code

        return availability_code

    # Έλεγχος για το λεξιλόγιο Frequency
    if display_name.startswith('http://publications.europa.eu/resource/authority/frequency/'):
        frequency_code = display_name.split('/')[-1]
        tags = _get_vocabulary_tags('Frequency')

        for tag in tags:
            if tag.get('value_uri') == display_name or tag.get('name') == frequency_code:
                return _get_label_by_language(tag) or frequency_code

        return frequency_code

        # Έλεγχος για το λεξιλόγιο Licence
    if display_name.startswith('http://publications.europa.eu/resource/authority/licence/'):
            licence_code = display_name.split('/')[-1]
            tags = _get_vocabulary_tags('Licence')

            import logging
            log = logging.getLogger(__name__)
            log.debug(f"LICENSE_DEBUG: Attempting to translate URL -> {display_name}")

            for tag in tags:
                log.debug(f"LICENSE_DEBUG: Checking against tag data -> {tag}")

                if tag.get('value_uri') == display_name or tag.get('name') == licence_code:
                    return tag.get('display_name', licence_code)

            return licence_code

    # Έλεγχος για facet dataset_type
    if display_name.startswith('data-service'):
        return 'API'
    if display_name.startswith('dataset'):
        return 'Σύνολο Δεδομένων' if lang_system == 'el' else 'Dataset'

    # Αν δεν ταιριάζει με κανένα από τα παραπάνω, επιστρέφουμε το αρχικό display_name
    return display_name


def vocabulary_facet_title(title):
    """
    Μέθοδος για αλλαγή του τίτλου facet.
    Ανακτά τα δεδομένα από τη βάση αντί για hardcoded τιμές.
    """
    lang_system = lang()

    # Αντιστοίχιση των facet τίτλων με τα vocabulary IDs
    vocabulary_mapping = {
        'access_rights': 'Access right',
        'theme': 'Data theme',
        'dcat_type': 'Dataset type',
        'hvd_category': 'High-value dataset categories',
        'frequency': 'Frequency',
        'availability': 'Planned availability',
        'license': 'Licence',
        'publishertype': {'el': 'Τύπος Οργανισμού', 'en': 'Organization Type'},
        'is_hvd': {'el': 'Σύνολο Δεδομένων Υψηλής Αξίας', 'en': 'High-Value Dataset'},
        'is_nsip': {'el': 'Σύνολο Δεδομένων NSIP', 'en': 'NSIP Dataset'},

    }

    # Αν ο τίτλος αντιστοιχεί σε ένα λεξιλόγιο, προσπαθούμε να πάρουμε την περιγραφή του
    if title in vocabulary_mapping:
        vocabulary_id = vocabulary_mapping[title]

        # Προσπαθούμε να πάρουμε την περιγραφή του λεξιλογίου
        # Αν αποτύχει, χρησιμοποιούμε τις προκαθορισμένες μεταφράσεις
        try:
            # Εδώ θα μπορούσαμε να χρησιμοποιήσουμε την περιγραφή του λεξιλογίου
            # αλλά προς το παρόν χρησιμοποιούμε τις προκαθορισμένες μεταφράσεις
            # για συμβατότητα με την υπάρχουσα υλοποίηση
            if title == 'access_rights':
                return 'Δικαιώματα πρόσβασης' if lang_system == 'el' else 'Access rights'
            elif title == 'theme':
                return 'Κατηγορίες' if lang_system == 'el' else 'Categories'
            elif title == 'dcat_type':
                return 'Τύποι' if lang_system == 'el' else 'Types'
            elif title == 'hvd_category':
                return 'Κατηγορίες HVD' if lang_system == 'el' else 'HVD Categories'
            elif title == 'frequency':
                return 'Συχνότητα' if lang_system == 'el' else 'Frequency'
            elif title == 'availability':
                return 'Διαθεσιμότητα' if lang_system == 'el' else 'Availability'
            elif title == 'license':
                return 'Άδειες' if lang_system == 'el' else 'Licenses'
            elif title == 'publishertype':
                return 'Τύπος Οργανισμού' if lang_system == 'el' else 'Organization Type'
            elif title == 'is_hvd':
                return 'Σύνολο Δεδομένων Υψηλής Αξίας' if lang_system == 'el' else 'High-Value Dataset'
            elif title == 'is_nsip':
                return 'Σύνολο Δεδομένων NSIP' if lang_system == 'el' else 'NSIP dataset'
        except Exception as e:
            log.exception(f'Error retrieving vocabulary description for "{vocabulary_id}": {e}')

    if title == 'dataset_type':
        return 'Υπηρεσία/Σύνολο Δεδομένων' if lang_system == 'el' else 'Service/Dataset'
    if title == 'tags':
        return 'Λέξεις-κλειδιά' if lang_system == 'el' else 'Keywords'
    if title == 'organization':
        return 'Οργανισμός' if lang_system == 'el' else 'Organization'
    if title == 'res_format':
        return 'Μορφότυποι' if lang_system == 'el' else 'Format'
    if title == 'qa_mqa_rating':
        return 'Ποιότητα μεταδεδομένων' if lang_system == 'el' else 'Metadata quality'
    if title == 'qa_openness_score':
        return 'Βαθμολογία Ανοιχτότητας' if lang_system == 'el' else 'Openness score'

    return title


def get_vocabulary_id_for_field(field_name):
    """
    Επιστρέφει το αναγνωριστικό του λεξιλογίου για ένα συγκεκριμένο πεδίο.
    Χρησιμοποιεί ένα mapping που θα μπορούσε να ανακτηθεί από τη βάση δεδομένων.
    """
    # Αντιστοίχιση των πεδίων με τα vocabulary IDs
    # Αυτό θα μπορούσε να ανακτηθεί από τη βάση δεδομένων σε μελλοντική έκδοση
    vocabulary_mapping = {
        'theme': 'Data theme',
        'dcat_type': 'Dataset type',
        'hvd_category': 'High-value dataset categories',
        'access_rights': 'Access right',
        'frequency': 'Frequency',
        'availability': 'Planned availability',
        'license': 'Licence',
        'publishertype': 'Publisher type'
    }

    # Προσπαθούμε να βρούμε το vocabulary ID για το συγκεκριμένο πεδίο
    vocabulary_id = vocabulary_mapping.get(field_name)

    if vocabulary_id:
        # Επαληθεύουμε ότι το vocabulary υπάρχει στη βάση δεδομένων
        try:
            # Χρησιμοποιούμε το cache για καλύτερη απόδοση
            if vocabulary_id in _vocabulary_cache:
                return vocabulary_id

            # Αν δεν υπάρχει στο cache, το ανακτούμε από τη βάση
            vocabulary_data = toolkit.get_action('vocabularyadmin_vocabulary_show')(
                {}, {'id': vocabulary_id}
            )
            # Αν φτάσουμε εδώ, το vocabulary υπάρχει
            return vocabulary_id
        except toolkit.ObjectNotFound:
            log.warning(f'Vocabulary not found: "{vocabulary_id}" for field "{field_name}"')
            return None
        except Exception as e:
            log.exception(f'Error retrieving vocabulary "{vocabulary_id}" for field "{field_name}": {e}')
            # Επιστρέφουμε το vocabulary_id ακόμα και αν υπάρχει σφάλμα
            # για να διατηρήσουμε τη συμβατότητα με την υπάρχουσα υλοποίηση
            return vocabulary_id

    return None


def build_mqa_nav_icon(pkg_id, dataset_type='dataset'):
    """
    Build the MQA tab navigation icon for the dataset view.

    Args:
        pkg_id: The ID of the dataset
        dataset_type: The type of the dataset (default: 'dataset')

    Returns:
        HTML for the MQA tab navigation icon
    """
    from ckan.lib.helpers import build_nav_icon
    return build_nav_icon(dataset_type + '_type.mqa', _('Metadata Quality'), id=pkg_id, package_type=dataset_type, icon='check-square')

def fluent_language_is_required(field, lang):
    """
    Return True if the given language is required for the field.
    This typically checks field['required_languages'] or a similar schema setting.
    """
    if not isinstance(field, dict):
        log.warning(f"Expected field to be dict, got {type(field)}: {field}")
        return False
    required_languages = field.get('required_languages', [])
    return lang in required_languages

def get_organizations_stats():
    """Returns statistics about organizations and their publisher types"""
    try:
        organizations = toolkit.get_action('organization_list')({}, {
            'all_fields': True,
            'include_extras': True
        })

        total_orgs = len(organizations)
        orgs_with_type = sum(1 for org in organizations
                             if org.get('publishertype'))

        return {
            'total': total_orgs,
            'with_type': orgs_with_type,
            'without_type': total_orgs - orgs_with_type,
            'type_percentage': round((orgs_with_type / total_orgs * 100) if total_orgs > 0 else 0, 1)
        }
    except Exception as e:
        log.error(f'Error getting organizations statistics: {str(e)}')
        return {
            'total': 0,
            'with_type': 0,
            'without_type': 0,
            'type_percentage': 0
        }

def get_access_rights_type():
    """
    Επιστρέφει το access_rights_type από το request αν υπάρχει.
    """
    from ckan.common import request
    return request.params.get('access_rights_type', '')

def get_config_as_bool(key, default=False):
    """
    Get configuration value as boolean.

    Args:
        key (str): Configuration key
        default (bool): Default value if key not found

    Returns:
        bool: Boolean value of the configuration
    """
    value = toolkit.config.get(key, default)
    return toolkit.asbool(value)

def get_config_value(key, default=""):
    """
    Retrieve a raw configuration value with an optional default.
    """
    value = toolkit.config.get(key)
    return value if value is not None else default


def _localize_data_service_label(text):
    """
    Post-process humanized strings for the data-service dataset type so the
    rendered labels match the active locale.
    """
    if not isinstance(text, str):
        return text

    current_lang = lang()
    if current_lang == 'el':
        replacements = {
            'Data-services': 'Υπηρεσίες Δεδομένων',
            'Data-service': 'Υπηρεσία Δεδομένων',
            'Data Services': 'Υπηρεσίες Δεδομένων',
            'Data Service': 'Υπηρεσία Δεδομένων',
        }
    else:
        replacements = {
            'Data-services': 'Data Services',
            'Data-service': 'Data Service',
        }

    for source, target in replacements.items():
        text = text.replace(source, target)

    if current_lang == 'el':
        phrase_replacements = {
            'My Υπηρεσίες Δεδομένων': 'Οι Υπηρεσίες Δεδομένων μου',
            'My Υπηρεσία Δεδομένων': 'Η Υπηρεσία Δεδομένων μου',
            'Create Υπηρεσία Δεδομένων': 'Δημιουργία Υπηρεσίας Δεδομένων',
            'Add Υπηρεσία Δεδομένων': 'Προσθήκη Υπηρεσίας Δεδομένων',
            'Save Υπηρεσία Δεδομένων': 'Αποθήκευση Υπηρεσίας Δεδομένων',
            'Update Υπηρεσία Δεδομένων': 'Ενημέρωση Υπηρεσίας Δεδομένων',
            'View Υπηρεσία Δεδομένων': 'Προβολή Υπηρεσίας Δεδομένων',
        }
        for source, target in phrase_replacements.items():
            text = text.replace(source, target)

        verb_replacements = {
            'Create ': 'Δημιουργία ',
            'Add ': 'Προσθήκη ',
            'Save ': 'Αποθήκευση ',
            'Update ': 'Ενημέρωση ',
            'View ': 'Προβολή ',
        }
        for source, target in verb_replacements.items():
            text = text.replace(source, target)
    return text


def humanize_entity_type(entity_type, object_type, purpose):
    """
    Delegate to CKAN's default helper and localize the data-service type labels.
    """
    base_value = core_helpers.humanize_entity_type(entity_type, object_type, purpose)
    if object_type != 'data-service':
        return base_value
    return _localize_data_service_label(base_value)


def should_hide_mqa_tab():
    """
    Ελέγχει αν πρέπει να κρυφτεί το MQA tab βάσει της παραμετροποίησης
    ckanext.data_gov_gr.dataset.hide_mqa_tab στο configuration file.

    Returns:
        bool: True αν πρέπει να κρυφτεί το tab, False διαφορετικά
              Το default είναι True αν δεν έχει δηλωθεί καθόλου
    """
    return get_config_as_bool('ckanext.data_gov_gr.dataset.hide_mqa_tab', default=True)

def should_disable_protected_data():
    """
    Ελέγχει αν πρέπει να απενεργοποιηθούν τα protected data βάσει της παραμετροποίησης
    ckanext.data_gov_gr.dataset.disable_protected_data στο configuration file.

    Returns:
        bool: True αν πρέπει να απενεργοποιηθούν τα protected data, False διαφορετικά
              Το default είναι True αν δεν έχει δηλωθεί καθόλου
    """
    return get_config_as_bool('ckanext.data_gov_gr.dataset.disable_protected_data', default=True)

def should_hide_azure_translation():
    """
    Ελέγχει αν πρέπει να κρυφτεί η azure translation λειτουργία βάσει της παραμετροποίησης
    ckanext.data_gov_gr.dataset.hide_azure_translation στο configuration file.

    Returns:
        bool: True αν πρέπει να κρυφτεί η azure translation, False διαφορετικά
              Το default είναι True αν δεν έχει δηλωθεί καθόλου
    """
    return get_config_as_bool('ckanext.data_gov_gr.dataset.hide_azure_translation', default=True)


def should_show_decision_menu():
    """
    Ελέγχει αν πρέπει να εμφανιστεί το Decision menu βάσει της παραμετροποίησης
    ckanext.data_gov_gr.menu.show_decision στο configuration file.

    Returns:
        bool: True αν πρέπει να εμφανιστεί το menu, False διαφορετικά
              Το default είναι True αν δεν έχει δηλωθεί καθόλου
    """
    return get_config_as_bool('ckanext.data_gov_gr.menu.show_decision', default=True)


def should_show_decision_button():
    """
    Ελέγχει αν πρέπει να εμφανιστεί το κουμπί προσθήκης Απόφασης στις σελίδες οργανισμών
    χρησιμοποιώντας την ίδια παράμετρο με το menu visibility.

    Returns:
        bool: True αν πρέπει να εμφανιστεί το κουμπί, False διαφορετικά
              Το default είναι True αν δεν έχει δηλωθεί καθόλου
    """
    return get_config_as_bool('ckanext.data_gov_gr.menu.show_decision', default=True)

def get_data_service_guides_url():
    """
    Return the configured URL for the data service guides reference.
    """
    return get_config_value('ckanext.data_gov_gr.data_service_guides_url')

def get_helpers():
    return {
        "vocabulary_facet_item_label": vocabulary_facet_item_label,
        "vocabulary_facet_title": vocabulary_facet_title,
        "get_vocabulary_id_for_field": get_vocabulary_id_for_field,
        "build_mqa_nav_icon": build_mqa_nav_icon,
        "fluent_language_is_required": fluent_language_is_required,
        "get_organizations_stats": get_organizations_stats,
        'get_access_rights_type': get_access_rights_type,
        'get_data_service_guides_url': get_data_service_guides_url,
        'get_config_as_bool': get_config_as_bool,
        'get_config_value': get_config_value,
        'humanize_entity_type': humanize_entity_type,
        'should_hide_mqa_tab': should_hide_mqa_tab,
        'should_disable_protected_data': should_disable_protected_data,
        'should_hide_azure_translation': should_hide_azure_translation,
        'should_show_decision_menu': should_show_decision_menu,
        'should_show_decision_button': should_show_decision_button
    }
