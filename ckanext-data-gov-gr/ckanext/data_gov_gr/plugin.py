import logging
from typing import Dict, Any

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import os
import sys

from ckan.common import config, request
from ckan import model
import requests
from urllib.parse import urlparse
import re

from ckanext.data_gov_gr import views
from ckanext.data_gov_gr.logic import validators, actions, auth
import json

from ckanext.keycloak.helpers import enable_internal_login
from ckanext.dcat.interfaces import IDCATRDFHarvester

plugin_dir = os.path.dirname(sys.modules[__name__].__file__)
import ckanext.data_gov_gr.helpers as helpers

log = logging.getLogger(__name__)


class DataGovGrPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IValidators)
    plugins.implements(plugins.IFacets)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(IDCATRDFHarvester)

    def _get_clean_license_id(self, license_uri_or_id):
        """
        Παίρνει ένα URI ή ID άδειας και επιστρέφει το "καθαρό" του ID.
        π.χ. 'http://.../AFL_3_0' -> 'AFL_3_0'
        π.χ. 'cc-by-4.0' -> 'cc-by-4.0'
        """
        if not license_uri_or_id or not isinstance(license_uri_or_id, str):
            return None

        # Αν είναι URI, παίρνουμε το τελευταίο μέρος
        if '/' in license_uri_or_id:
            return license_uri_or_id.split('/')[-1]

        # Αλλιώς, είναι ήδη καθαρό ID
        return license_uri_or_id

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("assets", "data_gov_gr")

        self._change_valid_name()

    def update_config_schema(self, schema):
        """
        Make the Power BI embed URL configurable from /ckan-admin/config.

        This defines runtime-editable configuration options for:
        - ``ckanext.data_gov_gr.powerbi_embed_url`` (Power BI)
        - ``ckanext.data_gov_gr.user_survey.url`` (user survey popup/link URL)
        - ``ckanext.data_gov_gr.showcase.disclaimer`` (apps/showcases disclaimer)
        - ``ckanext.data_gov_gr.dataset.legislation.open`` (default applicable legislation for open datasets)
        - ``ckanext.data_gov_gr.dataset.legislation.protected`` (default applicable legislation for protected datasets)
        which are independent from their fallback values in the ini file.

        Επιπλέον, ορίζει παραμετρικές επιλογές μενού για τα σύνολα δεδομένων:
        - ``ckanext.data_gov_gr.menu.dataset.items`` (JSON λίστα από αντικείμενα
          με πεδία ``label`` και ``query``)
        """
        ignore_missing = toolkit.get_validator('ignore_missing')
        unicode_safe = toolkit.get_validator('unicode_safe')

        schema.update({
            'ckanext.data_gov_gr.powerbi_embed_url': [ignore_missing, unicode_safe],
            'ckanext.data_gov_gr.user_survey.url': [ignore_missing, unicode_safe],
            'ckanext.data_gov_gr.showcase.disclaimer': [ignore_missing, unicode_safe],
            'ckanext.data_gov_gr.dataset.legislation.open': [ignore_missing, unicode_safe],
            'ckanext.data_gov_gr.dataset.legislation.protected': [ignore_missing, unicode_safe],
            # Νέα, JSON παραμετρικές επιλογές για dropdown συνόλων δεδομένων
            'ckanext.data_gov_gr.menu.dataset.items': [ignore_missing, unicode_safe],
        })

        return schema

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprint()

    # IDCATRDFHarvester

    def before_download(self, url, harvest_job):
        """
        Pass-through hook before downloading the remote RDF/JSON-LD document.
        We don't modify the URL at this stage.
        """
        return url, []

    def update_session(self, session):
        """
        Leave the HTTP session unchanged (no custom headers/certs needed here).
        """
        return session

    def after_download(self, content, harvest_job):
        """
        Tweak JSON-LD feeds so that `accessUrl` keys are normalised to
        `accessURL`, which is what the DCAT parser expects to map to
        dcat:accessURL.
        """
        if not content:
            return content, []

        try:
            if '"accessUrl"' in content:
                content = content.replace('"accessUrl"', '"accessURL"')
        except Exception:
            # In case of non-text content, just leave it untouched
            pass

        return content, []

    def after_parsing(self, rdf_parser, harvest_job):
        """
        No-op hook after the RDF/JSON-LD content has been parsed into a graph.
        """
        return rdf_parser, []

    def after_update(self, harvest_object, dataset_dict, temp_dict):
        """
        No-op hook after package_update.
        """
        return None

    def after_create(self, harvest_object, dataset_dict, temp_dict):
        """
        No-op hook after package_create.
        """
        return None

    def update_package_schema_for_create(self, package_schema):
        """
        Leave the package schema unchanged on create.
        """
        return package_schema

    def update_package_schema_for_update(self, package_schema):
        """
        Leave the package schema unchanged on update.
        """
        return package_schema

    # ΙValidators
    def get_validators(self) -> Dict[str, Any]:
        return validators.get_validators()

    # ITemplateHelpers

    def get_helpers(self):
        return helpers.get_helpers()

        self.raise_error_if_username_not_set()

    # IPackageController
    def before_index(self, pkg_dict):
        """
        Προσθέτουμε το 'publishertype' από τον συνδεδεμένο οργανισμό.
        Σημείωση: Δεν παραλείπουμε τα data-service ώστε να υπάρχει διαθέσιμο
        το πεδίο για φιλτράρισμα στο ευρετήριο υπηρεσιών.
        """
        # Skip processing only for decisions
        if pkg_dict.get('type') in ['decision']:
            return pkg_dict

        org_id = pkg_dict.get('owner_org')
        if org_id:
            try:
                org = model.Group.get(org_id)
                if org and org.is_organization:
                    publisher_type_value = org.extras.get('publishertype')
                    if publisher_type_value:
                        pkg_dict['publishertype'] = publisher_type_value
            except Exception:
                pass  # Αγνοούμε τυχόν σφάλματα για να μην σπάσει το indexing

        return pkg_dict

    def before_dataset_search(self, data_dict):

        # Αναζητούμε το χωρικό φίλτρο 'ext_bbox' στο αρχικό dictionary που έρχεται από το request.
        # Το φίλτρο αυτό αναμένεται να είναι μια συμβολοσειρά τεσσάρων δεκαδικών τιμών: minLon, minLat, maxLon, maxLat (π.χ. "24.1,35.2,25.9,36.1")
        ext_bbox_value = data_dict.get('ext_bbox')

        # Αν υπάρχει αυτό το φίλτρο, συνεχίζουμε
        if ext_bbox_value:

            # Μεταφέρουμε το 'ext_bbox' στα 'extras' ώστε να είναι συμβατό με το πώς CKAN χειρίζεται επιπλέον παραμέτρους.
            # Χρησιμοποιούμε setdefault για να δημιουργήσουμε το 'extras' αν δεν υπάρχει ήδη.
            data_dict.setdefault('extras', {})['ext_bbox'] = ext_bbox_value

            # Αφαιρούμε το αρχικό πεδίο από το root για να αποφύγουμε συγκρούσεις ή warnings
            data_dict.pop('ext_bbox', None)

            try:
                # Διαχωρίζουμε τις τιμές της συμβολοσειράς και τις μετατρέπουμε σε float
                minLng, minLat, maxLng, maxLat = map(float, ext_bbox_value.split(','))

                # Δημιουργούμε το φίλτρο αναζήτησης (FQ) για το Solr, με βάση τα όρια bounding box.
                # Η σύνταξη τύπου: minx:[* TO maxLng] AND maxx:[minLng TO *] εξασφαλίζει ότι η γεωμετρία του dataset, τέμνει το ορθογώνιο του χάρτη (δηλ. υπάρχει κάποια επικάλυψη).
                bbox_fq = (
                    f"(minx:[* TO {maxLng}] AND maxx:[{minLng} TO *] "
                    f"AND miny:[* TO {maxLat}] AND maxy:[{minLat} TO *])"
                )

                # Αν υπάρχει ήδη φίλτρο fq (π.χ. φίλτρο από οργανισμό ή κατηγορία), το συνδυάζουμε με AND
                if data_dict.get('fq'):
                    data_dict['fq'] = f"({data_dict['fq']}) AND {bbox_fq}"
                else:
                    # Διαφορετικά, το bbox φίλτρο είναι το μόνο
                    data_dict['fq'] = bbox_fq

            except Exception as e:
                # Αν υπάρξει σφάλμα (π.χ. κακό format στη συμβολοσειρά), το καταγράφουμε στο log
                log.warning(f"Invalid ext_bbox format in plugin: {ext_bbox_value} - {e}")

        # Επιστρέφουμε το τροποποιημένο data_dict ώστε να συνεχιστεί η αναζήτηση με τα νέα φίλτρα
        return data_dict

    def before_dataset_index(self, pkg_dict):
        """
        Εμπλουτισμός πεδίων για το ευρετήριο ανά τύπο πακέτου.
        - dataset: is_hvd, is_nsip, publishertype, resource extras
        - data-service: is_hvd, publishertype
        - decision: καμία επέμβαση
        """
        pkg_type = pkg_dict.get('type')

        # Καμία επέμβαση για αποφάσεις
        if pkg_type == 'decision':
            return pkg_dict

        # Publishertype από τον οργανισμό (dataset + data-service)
        try:
            org_id = pkg_dict.get('owner_org')
            if org_id:
                org = model.Group.get(org_id)
                if org and org.is_organization:
                    publisher_type_value = org.extras.get('publishertype')
                    if publisher_type_value:
                        if not publisher_type_value.startswith('http://'):
                            publisher_type_value = f'http://purl.org/adms/publishertype/{publisher_type_value}'
                        pkg_dict['publishertype'] = publisher_type_value
        except Exception as e:
            log.debug(f"Could not get org extras for {org_id}: {e}")

        # is_hvd (dataset + data-service)
        pkg_dict['is_hvd'] = 'No'
        if pkg_dict.get('hvd_category'):
            hvd_category_array_size = 0
            try:
                hvd_category_array = json.loads(pkg_dict['hvd_category'])
                hvd_category_array_size = len(hvd_category_array)
            except Exception as e:
                log.error(f"Unexpected error while processing hvd_category: {e}")
            if hvd_category_array_size > 0:
                pkg_dict['is_hvd'] = 'Yes'

        # is_nsip + resource extras μόνο για datasets
        if pkg_type == 'dataset':
            access_rights_value = pkg_dict.get('access_rights', '')
            if isinstance(access_rights_value, str):
                if access_rights_value.endswith('/NON_PUBLIC') or access_rights_value.endswith('/RESTRICTED'):
                    pkg_dict['is_nsip'] = 'Yes'
                elif access_rights_value.endswith('/PUBLIC'):
                    pkg_dict['is_nsip'] = 'No'

            extra_resource_fields = model.Resource.get_extra_columns()
            for field in extra_resource_fields:
                res_extras_key = f'res_extras_{field}'
                if res_extras_key in pkg_dict:
                    values = pkg_dict[res_extras_key]
                    if values:
                        pkg_dict[f'res_{field}'] = str(values)

        return pkg_dict

    def _extract_label_from_uri(self, uri):
        """Εξάγει το τελευταίο μέρος του URI για καλύτερη αναζήτηση"""
        if uri and isinstance(uri, str) and '/' in uri:
            # Παίρνουμε το τελευταίο μέρος του URI (πχ. 'PublicInstitution' από το 'http://purl.org/adms/publishertype/PublicInstitution')
            label = uri.split('/')[-1]

            # Μετατρέπουμε το camelCase σε λέξεις με κενά για καλύτερη αναγνωσιμότητα
            # π.χ. 'PublicInstitution' -> 'Public Institution'
            label = re.sub(r'([a-z])([A-Z])', r'\1 \2', label)
            return label
        return uri

    # IFacets

    def dataset_facets(self, facets_dict, package_type):
        """
        Προσθέτει τα facets που θέλουμε να εμφανίζονται στην αναζήτηση datasets.
        """
        # Αφαιρούμε το facet "groups"
        if 'groups' in facets_dict:
            del facets_dict['groups']

        # Προσθέτουμε τα facets που θέλουμε
        if package_type == 'dataset':
            facets_dict['is_hvd'] = toolkit._('High-Value Dataset')
            if not helpers.should_disable_protected_data():
                facets_dict['is_nsip'] = toolkit._('NSIP Dataset')
            else:
                facets_dict.pop('is_nsip', None)
            facets_dict['publishertype'] = toolkit._('Organization Type')

        elif package_type == 'data-service':
            facets_dict.pop('frequency', None)
            facets_dict.pop('res_format', None)
            facets_dict.pop('is_nsip', None)

        elif package_type == 'decision':
            facets_dict.pop('is_hvd',None)
            facets_dict.pop('publishertype',None)
            facets_dict.pop('access_rights',None)
            facets_dict.pop('frequency', None)
            facets_dict.pop('res_format', None)
            facets_dict.pop('is_nsip', None)

        return facets_dict

    def organization_facets(self, facets, group_type, extra_param):
        """
        Προσθέτει τα facets που θέλουμε να εμφανίζονται στην αναζήτηση organization pages.
        Εφαρμόζει τις ίδιες προσαρμογές με τα dataset facets για συνέπεια.
        """
        # Αφαιρούμε το facet "groups"
        if 'groups' in facets:
            del facets['groups']

        # Προσθέτουμε τα ίδια facets όπως στα datasets
        if not helpers.should_hide_mqa_tab():
            facets['qa_mqa_rating'] = toolkit._('Metadata quality')

        # Only add openness score facet for datasets, not for decisions or data-services
        # Since organization pages show all package types, we need to check if we're filtering by decisions or data-services
        request_path = request.environ.get('PATH_INFO', '')
        if 'decision' not in request_path and 'data-service' not in request_path:
            facets['qa_openness_score'] = toolkit._('Openness score')

        if not helpers.should_disable_protected_data():
            facets['is_nsip'] = toolkit._('NSIP Dataset')
        else:
            facets.pop('is_nsip', None)

        return facets

    #ITranslation
    def i18n_directory(self):
        return os.path.join(plugin_dir, 'i18n')

    def i18n_domain(self):
        return 'ckanext-data_gov_gr'

    def i18n_locales(self):
        return ['el', 'en', 'fr']

    # Implement IAuthFunctions
    def get_auth_functions(self):
        return {
            'check_user_org_permission': auth.check_user_org_permission,
            'user_organization_capacity': auth.user_organization_capacity_auth,
            'organization_list_with_user_extras': auth.organization_list_with_user_extras_auth
        }

    # IActions
    def get_actions(self):

        exposed_actions = {
            'check_user_org_permission': actions.check_user_org_permission,
            'organization_list_with_user_extras': actions.organization_list_with_user_extras,
            'user_organization_capacity': actions.user_organization_capacity,
            'geonames_search': self.geonames_search_action # Κλήση για ανάκτηση αποτελεσμάτων σε geoname
        }

        if not enable_internal_login():
            exposed_actions['user_invite'] = actions.user_invite_notify
            exposed_actions['organization_member_create'] = actions.organization_member_create_custom

        return exposed_actions

    ''' Έλεγχος αν είναι υπάρχει παραμετροποίηση '''
    def raise_error_if_username_not_set(self):
        username = config.get('ckanext.geonames.username')
        if not username:
            raise toolkit.ValidationError('GeoNames username is not configured.')
        return username

    from ckan.common import request
    ''' Κλήση για ανάκτηση περιοχής '''
    @staticmethod
    def geonames_search_action(context, data_dict):
        """
        CKAN Action to search GeoNames.
        Expects 'query' in data_dict.
        """
        query = data_dict.get('query')
        if not query:
            raise toolkit.ValidationError('Missing required parameter: query')



        try:
            # Ανάκτηση username και δημιουργία URL
            username = config.get('ckanext.geonames.username')

            language = DataGovGrPlugin.get_language_from_url_or_default()

            url = f"http://api.geonames.org/searchJSON?q={query}&maxRows=10&username={username}&lang={language}"
            # Ορίζουμε σαν timeout για να απαντήσει ο GeoNames Server
            REQUEST_TIMEOUT = (5, 10)  # seconds


            response = requests.get(url, timeout=REQUEST_TIMEOUT)

            # Exception για τις κλήσεις που είναι της μορφής (4xx or 5xx)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout as e:
            # Exception για τις κλήσεις που αργεί ο server να απαντήσει
            raise toolkit.ValidationError(f'GeoNames API request timed out: {str(e)}')
        except requests.exceptions.RequestException as e:
            # Exception για client side σφάλματα
            raise toolkit.ValidationError(f'Error communicating with GeoNames API: {str(e)}')
        except toolkit.ValidationError:
            # ΓΙα validation σφάλματα
            raise
        except Exception as e:
            # Για οποιοδήποτε άλλο σφάλμα
            raise toolkit.ValidationError(f'Unexpected error during GeoNames search: {str(e)}')

    ''' Ανάκτηση της γλώσσας από το URL αν υπάρχει '''
    @staticmethod
    def get_language_from_url_or_default():
        referer = request.environ.get('HTTP_REFERER')

        # Default to None initially
        language = None
        import json
        if referer:
            # Extract the path and split
            path = urlparse(referer).path
            parts = path.strip("/").split("/")

            # Get language from URL if available
            if parts:
                lang_candidate = parts[0]

                # Get offered languages and default from CKAN config
                available_languages = ['el', 'en','fr', 'es', 'it', 'ja']
                default_language = toolkit.config.get('locale_default', 'en')

                # Check if it's an offered language
                if lang_candidate in available_languages:
                    language = lang_candidate
                else:
                    language = default_language
            else:
                language = toolkit.config.get('default_locale', 'en')
        else:
            language = toolkit.config.get('default_locale', 'en')
        return language

    @staticmethod
    def _change_valid_name():
        # Έλεγχος αν επιτρέπεται οποιοσδήποτε χαρακτήρας π.χ. @ από το ckan.ini
        allow_any_character = toolkit.asbool(
            toolkit.config.get('ckanext.data_gov_gr.user.allow_any_character_in_username', False))

        # Τροποποίηση του VALID_NAME pattern στην κλάση User για να επιτρέπει όλους τους χαρακτήρες
        if allow_any_character:
            model.User.VALID_NAME = re.compile(r"^[^\s]{3,255}$")

    def _is_sysadmin(self, context):
        """
        Ελέγχει αν ο τρέχων χρήστης είναι sysadmin.
        Επιστρέφει True αν είναι sysadmin, αλλιώς False.
        Χρησιμοποιείται για να αποφασιστεί αν μπορεί να αλλάξει την ορατότητα σε δημόσια.
        """
        user = context.get('user')
        if not user:
            return False
        user_obj = model.User.get(user)
        return bool(user_obj and user_obj.sysadmin)

    def before_create(self, *args, **kwargs):
        """
        Συνδυασμένο hook:

        - Ως IDCATRDFHarvester.before_create(harvest_object, dataset_dict, temp_dict)
          (καλείται από τον DCATRDFHarvester πριν το package_create)
        - Ως IPackageController.before_create(context, pkg_dict)
          (καλείται από τον CKAN πριν τη δημιουργία dataset/decision)
          Αν το dataset είναι τύπου 'decision' και ο χρήστης **δεν** είναι sysadmin:
          - Επιβάλλει να είναι ιδιωτικό (private=True)
          - Αποτρέπει την επιλογή Public στο UI ή μέσω API
        """
        # Κλήση από DCATRDFHarvester (harvest_object, dataset_dict, temp_dict)
        if len(args) == 3 and args and not isinstance(args[0], dict):
            # Δεν χρειάζεται να κάνουμε κάτι ειδικά για το harvest case
            return

        # Κλήση από IPackageController (context, pkg_dict)
        if len(args) == 2 and isinstance(args[0], dict) and isinstance(args[1], dict):
            context, pkg_dict = args
            try:
                if pkg_dict.get('type') == 'decision':
                    if not self._is_sysadmin(context):
                        # "Κλειδώνουμε" το decision σε ιδιωτικό
                        pkg_dict['private'] = True
            except Exception:
                # Αγνοούμε σφάλματα για να μην σπάσει η δημιουργία dataset
                pass
            return pkg_dict

        # Οποιαδήποτε άλλη κλήση την αγνοούμε σιωπηλά
        return

    def before_update(self, *args, **kwargs):
        """
        Συνδυασμένο hook:

        - Ως IDCATRDFHarvester.before_update(harvest_object, dataset_dict, temp_dict)
          (καλείται από τον DCATRDFHarvester πριν το package_update)
        - Ως IPackageController.before_update(context, pkg_dict)
          (καλείται από τον CKAN πριν την ενημέρωση dataset/decision)
                  Καλείται πριν την ενημέρωση ενός dataset/decision.
        Αν το dataset είναι τύπου 'decision' και ο χρήστης **δεν** είναι sysadmin:
          - Επιβάλλει να παραμείνει ιδιωτικό (private=True)
          - Αποτρέπει την αλλαγή σε Public μέσω UI ή API
        """
        # Κλήση από DCATRDFHarvester (harvest_object, dataset_dict, temp_dict)
        if len(args) == 3 and args and not isinstance(args[0], dict):
            # Δεν κάνουμε κάτι επιπλέον για το harvest case
            return

        # Κλήση από IPackageController (context, pkg_dict)
        if len(args) == 2 and isinstance(args[0], dict) and isinstance(args[1], dict):
            context, pkg_dict = args
            try:
                if pkg_dict.get('type') == 'decision':
                    if not self._is_sysadmin(context):
                        # Απαγόρευση αλλαγής σε public για μη-sysadmin
                        pkg_dict['private'] = True
            except Exception:
                # Αγνοούμε σφάλματα για να μην σπάσει η ενημέρωση
                pass
            return pkg_dict

        # Οποιαδήποτε άλλη κλήση την αγνοούμε σιωπηλά
        return
