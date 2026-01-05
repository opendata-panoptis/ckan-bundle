import logging
import json
import re
from html import unescape as html_unescape
from datetime import datetime
import ckan.plugins.toolkit as toolkit
from ckan import model
from ckan.common import g
from ckan.lib import helpers as core_helpers
from ckan.lib.helpers import lang
from ckan.plugins.toolkit import render_snippet, _ # Import για το σύστημα μετάφρασης
from ckan.plugins.toolkit import _  # Import για το σύστημα μετάφρασης
from flask_login import current_user as _cu
from typing import (cast, Union)
from sqlalchemy import func

from ckanext.data_gov_gr.stats import DataGovStats


_MONTH_ABBREV_EN = {
    1: 'JAN',
    2: 'FEB',
    3: 'MAR',
    4: 'APR',
    5: 'MAY',
    6: 'JUN',
    7: 'JUL',
    8: 'AUG',
    9: 'SEP',
    10: 'OCT',
    11: 'NOV',
    12: 'DEC',
}

_MONTH_ABBREV_EL = {
    1: 'ΙΑΝ',
    2: 'ΦΕΒ',
    3: 'ΜΑΡ',
    4: 'ΑΠΡ',
    5: 'ΜΑΙ',
    6: 'ΙΟΥΝ',
    7: 'ΙΟΥΛ',
    8: 'ΑΥΓ',
    9: 'ΣΕΠ',
    10: 'ΟΚΤ',
    11: 'ΝΟΕ',
    12: 'ΔΕΚ',
}


def _month_abbrev(month: int, locale: str) -> str:
    locale_clean = (locale or '').lower()
    mapping = _MONTH_ABBREV_EL if locale_clean.startswith('el') else _MONTH_ABBREV_EN
    return mapping.get(int(month), str(month))


def _get_home_stats_catalog():
    """
    Σταθερός κατάλογος με τα διαθέσιμα στατιστικά που μπορούν να
    εμφανιστούν ως πλακίδια στην αρχική σελίδα.

    Κάθε στοιχείο επιστρέφεται ως dict με:
      - id: μοναδικό κλειδί ρύθμισης
      - route: Flask route name για το url_for
      - icon: CSS κλάση Font Awesome
      - title: μετάφραση τίτλου
      - description: σύντομη περιγραφή
    """
    return [
        {
            'id': 'datasets_by_theme',
            'route': 'stats.datasets_by_theme',
            'icon': 'fa fa-chart-pie',
            'title': _('Datasets Per Theme'),
            'description': _('Πλήθος συνόλων δεδομένων ανά θεματική κατηγορία.')
        },
        {
            'id': 'datasets_by_publisher_type',
            'route': 'dataset_type.stats_datasets_by_publisher_type',
            'icon': 'fa fa-sitemap',
            'title': _('Datasets Per Publisher Type'),
            'description': _('Πλήθος συνόλων δεδομένων ανά τύπο εκδότη.')
        },
        {
            'id': 'datasets_by_organization',
            'route': 'dataset_type.stats_datasets_per_organization',
            'icon': 'fa fa-building',
            'title': _('Datasets Per Organization'),
            'description': _('Πλήθος συνόλων δεδομένων ανά οργανισμό.')
        },
        {
            'id': 'datasets_vs_services',
            'route': 'dataset_type.stats_datasets_vs_services',
            'icon': 'fa fa-balance-scale',
            'title': _('Datasets vs Data Services'),
            'description': _('Σύγκριση μεταξύ συνόλων δεδομένων και υπηρεσιών δεδομένων.')
        },
        {
            'id': 'datasets_by_hvd_category',
            'route': 'dataset_type.stats_datasets_by_hvd_category',
            'icon': 'fa fa-star',
            'title': _('Datasets Per High-Value Category'),
            'description': _('Πλήθος HVD συνόλων δεδομένων ανά κατηγορία.')
        },
        {
            'id': 'organizations_by_publisher_type',
            'route': 'stats.organizations_by_publisher_type',
            'icon': 'fa fa-building',
            'title': _('Organizations Per Publisher Type'),
            'description': _('Πλήθος οργανισμών ανά τύπο εκδότη.')
        },
        {
            'id': 'total_datasets',
            'route': 'dataset_type.stats_total_datasets',
            'icon': 'fa fa-chart-line',
            'title': _('Total Number of Packages'),
            'description': _('Εξέλιξη του συνολικού αριθμού πακέτων στο χρόνο.')
        },
        {
            'id': 'dataset_revisions',
            'route': 'dataset_type.stats_dataset_revisions',
            'icon': 'fa fa-chart-area',
            'title': _('Package Revisions per Week'),
            'description': _('Μεταβολές και νέες δημοσιεύσεις πακέτων ανά εβδομάδα.')
        },
        {
            'id': 'most_edited',
            'route': 'dataset_type.stats_most_edited',
            'icon': 'fa fa-edit',
            'title': _('Most Edited Packages'),
            'description': _('Πακέτα με τις περισσότερες τροποποιήσεις.')
        },
        {
            'id': 'largest_groups',
            'route': 'dataset_type.stats_largest_groups',
            'icon': 'fa fa-users',
            'title': _('Largest Groups'),
            'description': _('Ομάδες με τα περισσότερα συνδεδεμένα πακέτα.')
        },
        {
            'id': 'top_tags',
            'route': 'dataset_type.stats_top_tags',
            'icon': 'fa fa-tags',
            'title': _('Top Tags'),
            'description': _('Δημοφιλέστερες ετικέτες συνόλων δεδομένων.')
        },
        {
            'id': 'top_creators',
            'route': 'dataset_type.stats_top_creators',
            'icon': 'fa fa-user',
            'title': _('Users Creating Most Datasets'),
            'description': _('Χρήστες που έχουν δημιουργήσει τα περισσότερα σύνολα δεδομένων.')
        },
        {
            'id': 'powerbi',
            'route': 'dataset_type.stats_powerbi',
            'icon': 'fa fa-chart-bar',
            'title': _('Power BI Αναφορές'),
            'description': _('Σύνθετες αναφορές και dashboards από Power BI.')
        },
    ]

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------

def google_analytics_snippet():
    return render_snippet("google_analytics/snippets/google_analytics.html")

# ---------------------------------------------------------------------------------------

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


def get_dataset_legislation_default():
    """
    Επιστρέφει την προεπιλεγμένη τιμή για το πεδίο Εφαρμοστέα Νομοθεσία
    κατά τη δημιουργία συνόλου δεδομένων, με βάση το access_rights_type.
    """
    access_type = get_access_rights_type()

    if access_type == 'open':
        # Προεπιλεγμένη νομοθεσία για ανοιχτά δεδομένα
        return get_config_value('ckanext.data_gov_gr.dataset.legislation.open', '')
    if access_type == 'protected':
        # Προεπιλεγμένη νομοθεσία για προστατευόμενα δεδομένα
        return get_config_value('ckanext.data_gov_gr.dataset.legislation.protected', 'DGA')

    return ''


def data_gov_gr_get_organizations():
    """
    Επιστρέφει λίστα οργανισμών για dropdown (id, name, title).
    """
    try:
        orgs = toolkit.get_action('organization_list')(
            {'ignore_auth': True},
            {'all_fields': True, 'include_extras': False}
        )
        # ταξινόμηση αλφαβητικά με βάση το title ή name
        orgs_sorted = sorted(
            orgs,
            key=lambda o: (o.get('title') or o.get('name') or '').lower()
        )
        return orgs_sorted
    except Exception as e:
        log.error(f'Error loading organizations for contact form: {e}')
        return []

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

    # Some runtime-edited values can end up as lists (eg hidden+checkbox
    # combinations). In that case, use the last submitted value.
    if isinstance(value, list):
        if not value:
            return default
        value = value[-1]

    try:
        return toolkit.asbool(value)
    except Exception:
        log.warning('Invalid boolean config %s=%r, using default=%r', key, value, default)
        return default

def get_config_value(key, default=""):
    """
    Retrieve a raw configuration value with an optional default.
    """
    value = toolkit.config.get(key)
    return value if value is not None else default


def get_powerbi_embed_url():
    """
    Return the configured Power BI embed URL.

    Priority:
    1. Runtime-editable admin config: ``ckanext.data_gov_gr.powerbi_embed_url``
    2. Fallback config file option: ``powerbi.embed_url``
    """
    # 1. Admin-configurable value from /ckan-admin/config
    admin_value = toolkit.config.get('ckanext.data_gov_gr.powerbi_embed_url')
    if admin_value:
        return admin_value.strip()

    # 2. Fallback to static config option in ckan.ini
    ini_value = toolkit.config.get('powerbi.embed_url')
    if ini_value:
        return ini_value.strip()

    return ""


def _resolve_dataset_item_url(raw_value: str) -> str | None:
    """
    Μετατρέπει την τιμή του πεδίου \"query\" σε πλήρες URL.

    Υποστηρίζει:
      - Πλήρες URL (http/https) -> επιστρέφεται όπως είναι
      - Σχετικό path που ξεκινά με \"/\" (π.χ. \"/dataset/?is_hvd=Yes\")
      - Path χωρίς αρχικό \"/\" (π.χ. \"dataset/?is_hvd=Yes\") που
        μετατρέπεται σε \"/dataset/?is_hvd=Yes\"
      - Μόνο το query μέρος (π.χ. \"fq=is_hvd:true\" ή \"?fq=is_hvd:true\"),
        οπότε το προσαρτά στο ``/dataset``.
    """
    raw = (raw_value or '').strip()
    if not raw:
        return None

    lower = raw.lower()
    if lower.startswith('http://') or lower.startswith('https://') or raw.startswith('/'):
        return raw

    # Υποστήριξη για τιμές τύπου \"dataset/?is_hvd=Yes\" χωρίς αρχικό '/'
    if raw.startswith('dataset'):
        return '/' + raw

    base_url = core_helpers.url_for('dataset.search')
    # Αφαιρούμε αρχικό '?' αν υπάρχει, για να ενώσουμε σωστά
    if raw.startswith('?'):
        raw = raw[1:]

    sep = '&' if '?' in base_url else '?'
    return f'{base_url}{sep}{raw}'

def get_dataset_menu_items():
    """
    Επιστρέφει τις παραμετρικές επιλογές του dropdown για τα σύνολα δεδομένων,
    όπως έχουν οριστεί από το /ckan-admin/config.

    Προτεραιότητα πηγών:

      1. Νέα JSON ρύθμιση ``ckanext.data_gov_gr.menu.dataset.items`` (δυναμικός αριθμός επιλογών),
         π.χ. ::

           [
             {\"label\": \"HVDs\", \"query\": \"fq=is_hvd:true\"},
             {\"label\": \"Ιστορικά\", \"query\": \"fq=dataset_type:historical\"}
           ]

         Όπου:
           - ``label``: το κείμενο που θα εμφανιστεί στο dropdown
           - ``query``: το κομμάτι του CKAN search query (π.χ. ``fq=...``)

    Επιστρέφει λίστα από dictionaries με πεδία:
      - ``label``: το κείμενο που θα εμφανιστεί
      - ``url``: πλήρες URL (είτε όπως δόθηκε, είτε προσαρμοσμένο στο ``/dataset``)
    """

    # Δυναμική ρύθμιση με JSON
    # Χρησιμοποιούμε το raw από το config ώστε να ξεχωρίζουμε
    # την περίπτωση «δεν έχει οριστεί καθόλου» (None) από την
    # περίπτωση «ορίστηκε αλλά είναι κενό/[]».
    raw = toolkit.config.get('ckanext.data_gov_gr.menu.dataset.items')
    if raw is not None:
        raw_str = str(raw).strip()
        if raw_str:
            try:
                parsed = json.loads(raw_str)
                items = []
                if isinstance(parsed, list):
                    for entry in parsed:
                        if not isinstance(entry, dict):
                            continue
                        label = (entry.get('label') or '').strip()
                        query = (entry.get('query') or '').strip()
                        if not label or not query:
                            continue

                        url = _resolve_dataset_item_url(query)
                        if not url:
                            continue

                        items.append({'label': label, 'url': url})

                # Ακόμη κι αν η λίστα είναι κενή, σεβόμαστε τη ρύθμιση JSON
                return items
            except Exception as e:
                log.exception('Error parsing ckanext.data_gov_gr.menu.dataset.items JSON: %s', e)
                # Σε περίπτωση σφάλματος επιστρέφουμε κενή λίστα
                return []
        else:
            # Έχει οριστεί το key αλλά είναι κενό -> σημαίνει
            # «καμία επιλογή» για το dropdown
            return []

    # Αν δεν έχει οριστεί καθόλου η JSON ρύθμιση, δεν εμφανίζονται επιλογές
    return []


def has_gitbook_pdf_export():
    """
    Check whether the GitBook PDF export configuration is complete.
    """
    space_id = get_config_value('ckanext.data_gov_gr.gitbook.space_id')
    token = get_config_value('ckanext.data_gov_gr.gitbook.api_token')
    return bool(space_id and token)


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


def allow_org_admins_public_decisions():
    """
    Ελέγχει αν οι διαχειριστές οργανισμών και οι εκδότες, μπορούν να δημιουργούν δημόσια Decisions.
    Αν η παράμετρος είναι κενή, σχόλιο, ή απουσιάζει, επιστρέφει True.
    """
    value = toolkit.config.get('ckanext.data_gov_gr.decision.allow_org_admins_public')

    # Αν η παράμετρος απουσιάζει, είναι None, κενή string, ή περιέχει μόνο σχόλιο
    if value is None:
        return True

    value_str = str(value).strip()

    # Αν είναι κενή ή ξεκινά με # (σχόλιο)
    if value_str == '' or value_str.startswith('#'):
        return True

    # Αν έχει τιμή, μετατρέπεται σε boolean
    return toolkit.asbool(value)

def extract_iframe_from_html(html):
    """
    Επιστρέφει ένα dict με:
    - body: το HTML χωρίς το πρώτο <iframe>...</iframe>
    - iframe: το πρώτο iframe μπλοκ (ή κενό string αν δεν βρεθεί)
    """
    if not isinstance(html, str) or '<iframe' not in html.lower():
        return {
            'body': html,
            'iframe': '',
        }

    pattern = re.compile(r'<iframe\b[^>]*>.*?</iframe>', re.IGNORECASE | re.DOTALL)
    match = pattern.search(html)
    if not match:
        return {
            'body': html,
            'iframe': '',
        }

    iframe_html = match.group(0)
    body_html = pattern.sub('', html, count=1)

    return {
        'body': body_html,
        'iframe': iframe_html,
    }

# ---------------------------------------------------------------------------------------

def dump_json(obj):
    """
    JSON dump helper safe for embedding in HTML attributes.
    """
    try:
        return json.dumps(obj, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(obj), ensure_ascii=False)


def _safe_url_for(endpoint: str, **kwargs) -> str | None:
    try:
        return toolkit.url_for(endpoint, **kwargs)
    except Exception:
        return None


def get_home_stats_tiles():
    """
    Return configured stats tiles for the home page (up to 4).

    Reads:
      - ckanext.data_gov_gr.home.stats.item1-4
    """
    catalog = _get_home_stats_catalog()
    catalog_by_id = {item['id']: item for item in catalog}

    selected: list[str] = []
    for idx in range(1, 5):
        key = f'ckanext.data_gov_gr.home.stats.item{idx}'
        raw_value = toolkit.config.get(key, '')
        if isinstance(raw_value, list):
            raw_value = raw_value[-1] if raw_value else ''

        stat_id = (raw_value or '').strip()
        if not stat_id or stat_id not in catalog_by_id:
            continue
        if stat_id in selected:
            continue
        selected.append(stat_id)

    return [catalog_by_id[stat_id] for stat_id in selected]


def get_home_total_datasets() -> int:
    """
    Return total number of public datasets.
    """
    try:
        res = toolkit.get_action('package_search')(
            {},
            {'q': '*:*', 'fq': 'dataset_type:dataset', 'rows': 0},
        )
        return int(res.get('count', 0) or 0)
    except Exception as e:
        log.error('Error counting total datasets: %s', e)
        return 0


def get_home_datasets_vs_services() -> dict:
    """
    Return counts for datasets vs data services.
    """
    try:
        stats = DataGovStats()
        return stats.datasets_vs_services()
    except Exception as e:
        log.error('Error loading datasets vs services: %s', e)
        return {'datasets': 0, 'data_services': 0}


def get_home_showcases(max_items=3):
    """
    Return up to ``max_items`` selected showcases for the home page.

    Values come from:
      - ckanext.data_gov_gr.home.showcases.ids (one showcase name/id per line)
    """
    try:
        limit = max(0, int(max_items))
    except Exception:
        limit = 3
    if not limit:
        return []

    raw_ids = get_config_value('ckanext.data_gov_gr.home.showcases.ids', '')
    if not raw_ids:
        return []

    # Handle values that can be lists (eg multiple submissions)
    if isinstance(raw_ids, list):
        raw_ids = raw_ids[-1] if raw_ids else ''

    ids_source = str(raw_ids).replace('\\n', '\n')
    candidates = [line.strip() for line in ids_source.splitlines() if line.strip()]
    if not candidates:
        candidates = [part.strip() for part in ids_source.split(',') if part.strip()]
    if not candidates:
        return []

    context = {'ignore_auth': True}
    showcases = []
    seen = set()
    for showcase_name in candidates:
        if showcase_name in seen:
            continue
        seen.add(showcase_name)
        if len(showcases) >= limit:
            break
        try:
            pkg = toolkit.get_action('package_show')(context, {'id': showcase_name})
        except Exception:
            continue

        if pkg.get('type') != 'showcase' or pkg.get('state') != 'active':
            continue
        if pkg.get('private'):
            continue

        # Respect approval workflow if present
        approval_status = None
        extras_list = pkg.get('extras') or []
        if isinstance(extras_list, list):
            for extra in extras_list:
                if isinstance(extra, dict) and extra.get('key') == 'approval_status':
                    approval_status = extra.get('value')
                    break
        approval_status = approval_status or pkg.get('extras_approval_status')
        if approval_status and approval_status != 'approved':
            continue

        title = pkg.get('title') or pkg.get('name')
        notes = pkg.get('notes') or ''

        image_display_url = pkg.get('image_display_url')
        image_url = pkg.get('image_url')
        if not image_display_url and image_url:
            if isinstance(image_url, str) and image_url and not image_url.startswith('http'):
                try:
                    image_display_url = core_helpers.url_for_static(
                        f'uploads/showcase/{image_url}', qualified=True
                    )
                except Exception:
                    image_display_url = None
            else:
                image_display_url = image_url

        showcases.append({
            'name': pkg.get('name'),
            'title': title,
            'notes': notes,
            'image_url': image_display_url,
        })

    return showcases


def get_home_featured_dataset_views(max_items=6):
    """
    Return up to ``max_items`` selected dataset resource views for the home page.

    Values come from:
      - ckanext.data_gov_gr.home.featured_dataset_views.ids (one view ID per line)
    """
    try:
        limit = max(0, int(max_items))
    except Exception:
        limit = 6
    if not limit:
        return []

    raw_ids = get_config_value('ckanext.data_gov_gr.home.featured_dataset_views.ids', '')
    if not raw_ids:
        return []

    if isinstance(raw_ids, list):
        raw_ids = raw_ids[-1] if raw_ids else ''

    ids_source = str(raw_ids).replace('\\n', '\n')
    candidates = [line.strip().strip(',') for line in ids_source.splitlines() if line.strip().strip(',')]
    if not candidates:
        candidates = [part.strip() for part in ids_source.split(',') if part.strip()]
    if not candidates:
        return []

    view_ids: list[str] = []
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        view_ids.append(candidate)
        if len(view_ids) >= limit:
            break

    context = {
        'user': getattr(g, 'user', None),
        'auth_user_obj': getattr(g, 'userobj', None),
    }

    featured = []
    for view_id in view_ids:
        try:
            resource_view = toolkit.get_action('resource_view_show')(context, {'id': view_id})
        except toolkit.ObjectNotFound:
            log.warning('Home featured dataset view "%s" not found', view_id)
            continue
        except toolkit.NotAuthorized:
            continue
        except Exception as e:
            log.error('Error loading home featured dataset view "%s": %s', view_id, e)
            continue

        resource_id = resource_view.get('resource_id')
        if not resource_id:
            continue

        try:
            resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
        except toolkit.ObjectNotFound:
            continue
        except toolkit.NotAuthorized:
            continue
        except Exception as e:
            log.error('Error loading resource for home view "%s": %s', view_id, e)
            continue

        package_id = resource.get('package_id')
        if not package_id:
            continue

        try:
            package = toolkit.get_action('package_show')(context, {'id': package_id})
        except toolkit.ObjectNotFound:
            continue
        except toolkit.NotAuthorized:
            continue
        except Exception as e:
            log.error('Error loading package for home view "%s": %s', view_id, e)
            continue

        package_type = package.get('type') or 'dataset'
        package_name = package.get('name') or package.get('id')
        dataset_title = package.get('title') or package_name or ''

        link = ''
        embed_src = ''
        try:
            link = toolkit.url_for(
                f'{package_type}_resource.read',
                id=package_name,
                resource_id=resource.get('id'),
            ) + f"?view_id={resource_view.get('id')}"
            embed_src = toolkit.url_for(
                f'{package_type}_resource.view',
                id=package_name,
                resource_id=resource.get('id'),
                view_id=resource_view.get('id'),
            )
        except Exception:
            pass

        try:
            iframed = core_helpers.resource_view_is_iframed(resource_view)
        except Exception:
            iframed = True

        featured.append({
            'id': resource_view.get('id') or view_id,
            'title': (resource_view.get('title') or '').strip() or dataset_title,
            'description': (resource_view.get('description') or '').strip(),
            'dataset_title': dataset_title,
            'link': link,
            'embed_src': embed_src,
            'iframed': iframed,
            'view': resource_view,
            'resource': resource,
            'package': package,
        })

    return featured


def get_available_showcases(limit=200):
    """
    Return list of available showcases for selection in /ckan-admin/config.
    """
    try:
        q = (
            model.Session.query(model.Package)
            .filter(model.Package.type == 'showcase')
            .filter(model.Package.state == 'active')
            .order_by(model.Package.title.asc())
        )
        if limit:
            q = q.limit(int(limit))
        results = q.all()
    except Exception as e:
        log.error('Error listing showcases for admin config: %s', e)
        return []

    available = []
    for pkg in results:
        if getattr(pkg, 'private', False):
            continue
        try:
            approval_status = pkg.extras.get('approval_status')
        except Exception:
            approval_status = None
        if approval_status and approval_status != 'approved':
            continue

        available.append({
            'name': pkg.name,
            'title': pkg.title or pkg.name,
        })

    return available


def _strip_html(text: str) -> str:
    cleaned = re.sub(r'<[^>]+>', ' ', text or '')
    cleaned = html_unescape(cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def get_home_news_items(max_items=3):
    """
    Return latest blog posts from ckanext-pages (/blog).
    """
    try:
        limit = max(0, int(max_items))
    except Exception:
        limit = 3
    if not limit:
        return []

    context = {
        'user': getattr(g, 'user', None),
        'auth_user_obj': getattr(g, 'userobj', None),
    }

    try:
        blog_list = toolkit.get_action('ckanext_pages_list')(
            context,
            {
                'order_publish_date': True,
                'page_type': 'blog',
                'private': False,
            },
        )
    except Exception as e:
        log.error('Error loading blog posts for home page: %s', e)
        return []

    items = []
    for post in blog_list or []:
        if len(items) >= limit:
            break

        name = (post or {}).get('name')
        if not name:
            continue

        title = (post.get('title') or name).strip()

        summary = (post.get('summary') or post.get('excerpt') or '').strip()
        if not summary:
            summary = _strip_html(post.get('content') or '')

        publish_date = post.get('publish_date')
        dt = None
        if publish_date:
            try:
                dt = datetime.fromisoformat(str(publish_date).replace('Z', '+00:00'))
            except Exception:
                dt = None

        date_day = date_month = date_year = date_short = None
        if dt:
            date_day = str(dt.day)
            date_month = _month_abbrev(dt.month, lang())
            date_year = str(dt.year)
            date_short = f'{dt.day}/{dt.month}'

        link = _safe_url_for('pages.blog_show', page=name) or f'/{lang()}/blog/{name}'

        items.append({
            'title': title,
            'link': link,
            'summary': summary,
            'date_day': date_day,
            'date_month': date_month,
            'date_year': date_year,
            'date_short': date_short,
        })

    return items


def _format_counter(value: int) -> str:
    try:
        return f"{int(value):,}".replace(',', ' ')
    except Exception:
        return str(value)


def get_home_portal_numbers():
    """
    Return EU-style counters for the home page.

    Controlled by:
      - ckanext.data_gov_gr.home.portal_numbers.enabled
    """
    if not get_config_as_bool('ckanext.data_gov_gr.home.portal_numbers.enabled', False):
        return []

    def _count_packages(fq: str) -> int:
        try:
            res = toolkit.get_action('package_search')({}, {'q': '*:*', 'fq': fq, 'rows': 0})
            return int(res.get('count', 0) or 0)
        except Exception:
            return 0

    datasets_count = get_home_total_datasets()
    apis_count = _count_packages('dataset_type:data-service')
    decisions_count = _count_packages('dataset_type:decision')

    try:
        orgs_count = int(
            model.Session.query(func.count(model.Group.id))
            .filter(model.Group.type == 'organization')
            .filter(model.Group.state == 'active')
            .scalar()
            or 0
        )
    except Exception:
        orgs_count = 0

    # Count only approved showcases (what the public sees), even for admins.
    showcases_count = _count_packages('+dataset_type:showcase +extras_approval_status:approved')

    try:
        blog_posts = toolkit.get_action('ckanext_pages_list')(
            {'ignore_auth': True},
            {'page_type': 'blog', 'private': False},
        )
        publications_count = len(blog_posts or [])
    except Exception:
        publications_count = 0

    return [
        {
            'value': _format_counter(datasets_count),
            'label': _('Σύνολα δεδομένων'),
            'link': _safe_url_for('dataset.search') or f'/{lang()}/dataset',
        },
        {
            'value': _format_counter(apis_count),
            'label': _('APIs'),
            'link': _safe_url_for('data-service.search') or f'/{lang()}/data-service',
        },
        {
            'value': _format_counter(decisions_count),
            'label': _('Αποφάσεις'),
            'link': _safe_url_for('decision.search') or f'/{lang()}/decision',
        },
        {
            'value': _format_counter(orgs_count),
            'label': _('Φορείς'),
            'link': _safe_url_for('organization.index') or f'/{lang()}/organization',
        },
        {
            'value': _format_counter(showcases_count),
            'label': _('Εφαρμογές'),
            'link': _safe_url_for('showcase_blueprint.index') or f'/{lang()}/showcase',
        },
        {
            'value': _format_counter(publications_count),
            'label': _('Δημοσιεύσεις'),
            'link': _safe_url_for('pages.blog_index') or f'/{lang()}/blog',
        },
    ]


def get_stat_data(stat_id, raw_data=None, variant='full'):
    """
    Return normalized chart payload for a given stats ID.

    Used both on /stats pages (raw_data provided) and on home previews.
    """
    catalog = {item['id']: item for item in _get_home_stats_catalog()}
    meta = catalog.get(stat_id)
    if not meta:
        return None

    variant_norm = str(variant or 'full').lower()

    if raw_data is None:
        stats = DataGovStats()
        try:
            if stat_id == 'datasets_by_theme':
                raw_data = stats.datasets_by_theme()
            elif stat_id == 'datasets_by_publisher_type':
                raw_data = stats.datasets_by_publisher_type()
            elif stat_id == 'datasets_by_organization':
                raw_data = stats.datasets_by_organization()
            elif stat_id == 'datasets_vs_services':
                raw_data = stats.datasets_vs_services()
            elif stat_id == 'datasets_by_hvd_category':
                raw_data = stats.datasets_by_hvd_category()
            elif stat_id == 'organizations_by_publisher_type':
                raw_data = stats.organizations_by_publisher_type()
            elif stat_id == 'total_datasets':
                raw_data = stats.get_num_packages_by_week()
            elif stat_id == 'dataset_revisions':
                raw_data = {
                    'revisions': stats.get_by_week('package_revisions'),
                    'new_packages': stats.get_by_week('new_packages'),
                }
            elif stat_id == 'most_edited':
                raw_data = stats.most_edited_packages()
            elif stat_id == 'largest_groups':
                raw_data = stats.largest_groups()
            elif stat_id == 'top_tags':
                raw_data = stats.top_tags()
            elif stat_id == 'top_creators':
                raw_data = stats.top_package_creators()
            elif stat_id == 'powerbi':
                return None
        except Exception as e:
            log.error('Error loading stat data %s: %s', stat_id, e)
            return None

    link = _safe_url_for(meta.get('route')) if meta.get('route') else None

    def _pie_from_tuples(rows):
        data = []
        for code, label, count in rows or []:
            name = (label or code or '').strip()
            data.append({'name': name, 'value': int(count or 0)})
        return data

    def _bar_payload(categories, values, series_name):
        return {
            'categories': categories,
            'series': [
                {'name': series_name, 'data': values}
            ],
        }

    if stat_id in (
        'datasets_by_theme',
        'datasets_by_publisher_type',
        'datasets_vs_services',
        'datasets_by_hvd_category',
        'organizations_by_publisher_type',
    ):
        if stat_id == 'datasets_vs_services':
            data = [
                {'name': _('Datasets'), 'value': int(getattr(raw_data, 'datasets', None) or raw_data.get('datasets', 0) or 0)},
                {'name': _('Data Services'), 'value': int(getattr(raw_data, 'data_services', None) or raw_data.get('data_services', 0) or 0)},
            ]
        else:
            data = _pie_from_tuples(raw_data)

        # Keep previews compact by limiting slices a bit
        if variant_norm == 'preview' and len(data) > 10:
            data = data[:10]

        return {
            'id': stat_id,
            'type': 'pie',
            'title': meta.get('title'),
            'data': data,
            'link': link,
        }

    if stat_id in ('datasets_by_organization', 'most_edited', 'largest_groups', 'top_tags', 'top_creators'):
        categories = []
        values = []

        if stat_id == 'datasets_by_organization':
            for _org_id, org_title, num in raw_data or []:
                categories.append(org_title)
                values.append(int(num or 0))
        elif stat_id == 'most_edited':
            for pkg, num in raw_data or []:
                title = getattr(pkg, 'title', None) or getattr(pkg, 'name', None) or ''
                categories.append(title)
                values.append(int(num or 0))
        elif stat_id == 'largest_groups':
            for grp, num in raw_data or []:
                title = ''
                if grp is not None:
                    title = getattr(grp, 'title', None) or getattr(grp, 'name', None) or ''
                categories.append(title or _('Unknown'))
                values.append(int(num or 0))
        elif stat_id == 'top_tags':
            for tag, num in raw_data or []:
                title = ''
                if tag is not None:
                    title = getattr(tag, 'display_name', None) or getattr(tag, 'name', None) or ''
                categories.append(title or _('Unknown'))
                values.append(int(num or 0))
        elif stat_id == 'top_creators':
            for user, num in raw_data or []:
                title = ''
                if user is not None:
                    title = getattr(user, 'display_name', None) or getattr(user, 'name', None) or ''
                categories.append(title or _('Unknown'))
                values.append(int(num or 0))

        if variant_norm == 'preview' and len(categories) > 8:
            categories = categories[:8]
            values = values[:8]

        return {
            'id': stat_id,
            'type': 'bar',
            'title': meta.get('title'),
            'data': _bar_payload(categories, values, meta.get('title') or ''),
            'link': link,
        }

    if stat_id == 'total_datasets':
        points = []
        for week_date, _num_pkgs, cumulative in raw_data or []:
            points.append([week_date, int(cumulative or 0)])

        if variant_norm == 'preview' and len(points) > 24:
            points = points[-24:]

        return {
            'id': stat_id,
            'type': 'line',
            'title': meta.get('title'),
            'data': {
                'xAxisType': 'time',
                'series': [
                    {'name': _('Total datasets'), 'data': points},
                ],
            },
            'link': link,
        }

    if stat_id == 'dataset_revisions':
        revisions = None
        new_packages = None
        if isinstance(raw_data, dict):
            revisions = raw_data.get('revisions')
            new_packages = raw_data.get('new_packages')
        if revisions is None:
            revisions = []
        if new_packages is None:
            new_packages = []

        series_revisions = []
        for week_date, _pkg_ids, num, _cumulative in revisions or []:
            series_revisions.append([week_date, int(num or 0)])

        series_new = []
        for week_date, _pkg_ids, num, _cumulative in new_packages or []:
            series_new.append([week_date, int(num or 0)])

        if variant_norm == 'preview':
            if len(series_revisions) > 24:
                series_revisions = series_revisions[-24:]
            if len(series_new) > 24:
                series_new = series_new[-24:]

        return {
            'id': stat_id,
            'type': 'line',
            'title': meta.get('title'),
            'data': {
                'xAxisType': 'time',
                'series': [
                    {'name': _('All dataset revisions'), 'data': series_revisions},
                    {'name': _('New datasets'), 'data': series_new},
                ],
            },
            'link': link,
        }

    return None


def is_email_changed(data_dict, current_user):
    """
    Helper function to check if the email in data_dict differs from current user's email.

    Args:
        data_dict: Dictionary containing form data with email field
        current_user: Current logged in user object

    Returns:
        bool: True if email has changed, False otherwise
    """
    if not data_dict or not current_user:
        return False

    new_email = data_dict.get('email', '').strip()
    current_email = getattr(current_user, 'email', '').strip()

    return new_email != current_email

def should_show_update_button_in_user_profile(data_dict):
    """
    Καθορίζει αν πρέπει να εμφανιστούν τα κουμπιά διαχείρισης στο edit profile.

    Λογική:
    - Αν το email δεν έχει αλλάξει, εμφανίζονται τα κουμπιά
    - Αν το email έχει αλλάξει, κρύβονται τα κουμπιά

    Args:
        data_dict: Λεξικό με τα δεδομένα της φόρμας

    Returns:
        bool: True αν πρέπει να εμφανιστούν τα κουμπιά διαχείρισης
    """

    # Έλεγχος αν το internal login είναι απενεργοποιημένο
    try:
        # Κλήση του helper από το keycloak plugin
        if toolkit.h.enable_internal_login():
            return True  # Εμφάνισε πάντα τα κουμπιά αν υπάρχει internal login
    except (AttributeError, KeyError):
        # Το keycloak plugin δεν είναι εγκατεστημένο ή ο helper δεν είναι διαθέσιμος
        pass

    # Λήψη του τρέχοντος συνδεδεμένου χρήστη
    current_user = cast(Union["Model.User", "Model.AnonymousUser"], _cu)

    # If email hasn't changed, show buttons
    # Η λογική έχει αντληθεί από το main ckan, δες ckan/common.py
    if not is_email_changed(data_dict, current_user):
        return True

    # Hide buttons if email changed
    return False

# ---------------------------------------------------------------------------------------

def get_helpers():
    return {
        "vocabulary_facet_item_label": vocabulary_facet_item_label,
        "vocabulary_facet_title": vocabulary_facet_title,
        "get_vocabulary_id_for_field": get_vocabulary_id_for_field,
        "google_analytics_snippet": google_analytics_snippet,
        "build_mqa_nav_icon": build_mqa_nav_icon,
        "fluent_language_is_required": fluent_language_is_required,
        "get_organizations_stats": get_organizations_stats,
        'get_access_rights_type': get_access_rights_type,
        'get_dataset_legislation_default': get_dataset_legislation_default,
        'data_gov_gr_get_organizations': data_gov_gr_get_organizations,
        'get_data_service_guides_url': get_data_service_guides_url,
        'get_config_as_bool': get_config_as_bool,
        'get_config_value': get_config_value,
        'get_powerbi_embed_url': get_powerbi_embed_url,
        'get_home_stats_tiles': get_home_stats_tiles,
        'get_home_datasets_vs_services': get_home_datasets_vs_services,
        'get_home_total_datasets': get_home_total_datasets,
        'get_home_showcases': get_home_showcases,
        'get_home_featured_dataset_views': get_home_featured_dataset_views,
        'get_available_showcases': get_available_showcases,
        'get_home_news_items': get_home_news_items,
        'get_home_portal_numbers': get_home_portal_numbers,
        'has_gitbook_pdf_export': has_gitbook_pdf_export,
        'humanize_entity_type': humanize_entity_type,
        'should_hide_mqa_tab': should_hide_mqa_tab,
        'should_disable_protected_data': should_disable_protected_data,
        'should_hide_azure_translation': should_hide_azure_translation,
        'should_show_decision_menu': should_show_decision_menu,
        'should_show_decision_button': should_show_decision_button,
        'allow_org_admins_public_decisions': allow_org_admins_public_decisions,
        'should_show_update_button_in_user_profile': should_show_update_button_in_user_profile,
        'get_dataset_menu_items': get_dataset_menu_items,
        'extract_iframe_from_html': extract_iframe_from_html,
        'get_stat_data': get_stat_data,
        'dump_json': dump_json,
    }
