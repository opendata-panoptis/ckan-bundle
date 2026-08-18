import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import json
import logging

from ckan.common import _, c, g
from ckan.model.package_relationship import PackageRelationship
from pprint import pprint
from sqlalchemy import and_

abort = base.abort
get_action = logic.get_action
log = logging.getLogger(__name__)
import traceback


def get_relationships(id, context=None):
    # This appears to be needed for the click command...
    # ckan search-index rebuild
    # ...to work
    if not context:
        user = logic.get_action(u'get_site_user')(
            {u'model': model, u'ignore_auth': True}, {})
        context = {u'model': model, u'session': model.Session,
                   u'user': user[u'name']}

    relationships = []

    try:
        relationships = get_action('package_relationships_list')(context, {'id': id})
    except Exception as e:
        log.error(str(e))
        # whatever
        # @TODO: why does it not throw an exception here?

    if relationships:
        valid_relationships = []

        try:
            for relationship in relationships:
                # log.debug(relationship)
                if relationship.get('object'):
                    # QDES: handle standard CKAN dataset to dataset relationships
                    package = get_action('package_show')(context, {'id': relationship['object']})
                    if package:
                        relationship['title'] = package['title']
                    valid_relationships.append(relationship)
                elif relationship.get('comment'):
                    # QDES: handle CKAN dataset to EXTERNAL URI relationships
                    relationship['title'] = relationship['comment']
                    valid_relationships.append(relationship)
                else:
                    log.warning(
                        'Skipping package relationship without object or URI comment for package %s: %s',
                        id,
                        relationship
                    )
        except Exception as e:
            log.error(str(e))

        return valid_relationships
    else:
        return []


def get_relatable_datasets_from_db(source_package, same_org_only):
    owner_org = source_package.get('owner_org')

    query = model.Session.query(
        model.Package.name,
        model.Package.title,
        model.PackageExtra.value
    ).outerjoin(
        model.PackageExtra,
        and_(
            model.PackageExtra.package_id == model.Package.id,
            model.PackageExtra.key == 'title_translated',
            model.PackageExtra.state == 'active'
        )
    ).filter(
        model.Package.type == 'dataset',
        model.Package.state == 'active',
        model.Package.private == False
    )

    if same_org_only and owner_org:
        query = query.filter(model.Package.owner_org == owner_org)

    current_lang = h.lang()
    base_lang = current_lang.split('_')[0]
    pkg_list = []

    for package_name, title, title_translated_value in query:
        package_title = package_name
        title_translated = None

        if title_translated_value:
            try:
                title_translated = json.loads(title_translated_value)
            except ValueError:
                pass

        if isinstance(title_translated, dict):
            if title_translated.get(current_lang):
                package_title = title_translated[current_lang]
            elif title_translated.get(base_lang):
                package_title = title_translated[base_lang]
            elif title_translated.get('el'):
                package_title = title_translated['el']
            elif title:
                package_title = title
        elif title:
            package_title = title

        pkg_list.append({
            'name': package_name,
            'title': f"{package_title} ({package_name})"
        })

    return pkg_list


def get_relatable_datasets(id):

    relatable_datasets = []

    context = {'model': model, 'session': model.Session,
               'user': c.user, 'for_view': True,
               'ignore_auth': True,
               'auth_user_obj': c.userobj}

    try:
        source_package = get_action('package_show')(context, {'id': id})
        same_org_only = toolkit.asbool(toolkit.config.get(
            'ckanext.relationships.relatable_datasets_same_org_only',
            True
        ))
        use_db_query = toolkit.asbool(toolkit.config.get(
            'ckanext.relationships.relatable_datasets_use_db_query',
            True
        ))
        owner_org = source_package.get('owner_org')
        fq = 'type:dataset'

        if same_org_only and owner_org:
            fq += ' owner_org:"{}"'.format(owner_org)

        if use_db_query:
            pkg_list = get_relatable_datasets_from_db(source_package, same_org_only)
        else:
            # Παίρνουμε μόνο τον αριθμό των datasets
            count_params = {
                'q': '*:*',
                'fq': fq,
                'rows': 0  # Δεν θέλουμε αποτελέσματα, μόνο το count
            }

            count_result = get_action('package_search')(context, count_params)
            total_datasets = count_result['count']

            # Χρησιμοποιούμε package_search για να πάρουμε όλα τα datasets
            search_params = {
                'q': '*:*',  # Όλα τα datasets
                'fq': fq,
                'rows': total_datasets
            }

            search_results = get_action('package_search')(context, search_params)
            packages = search_results['results']

            pkg_list = []

            for package in packages:
                package_title = package.get('name', 'Untitled')

                if 'title_translated' in package and isinstance(package['title_translated'], dict):
                    # Παίρνουμε την τρέχουσα γλώσσα
                    current_lang = h.lang()

                    # Προτιμάμε την τρέχουσα γλώσσα
                    if current_lang in package['title_translated'] and package['title_translated'][current_lang]:
                        package_title = package['title_translated'][current_lang]
                    # Αν δεν υπάρχει η τρέχουσα γλώσσα, επιστρέφουμε στα ελληνικά (default)
                    else:
                        package_title = package['title_translated']['el']

                # Προσθέτουμε το (name) στον τίτλο για καλύτερη αναγνώριση
                display_title = f"{package_title} ({package['name']})"

                result_dict = {
                    'name': package['name'],
                    'title': display_title,
                    'match_field': 'title',
                    'match_displayed': package_title
                }
                pkg_list.append(result_dict)

    except Exception:
        traceback.print_exc()  # This prints the full traceback to the console
        abort(500, _('An issue occurred'))  # Generic message to the user

    if pkg_list:
        # get the current relationships, so we can exclude those datasets from the list
        existing_relationships = [relationship['object'] for relationship in get_relationships(id)]

        for package in pkg_list:
            if package['name'] != source_package['name'] and package['name'] not in existing_relationships:
                relatable_datasets.append(
                    {
                        'name': package['name'],
                        'title': package['title']
                    }
                )

    return relatable_datasets


def get_lineage_notes(type, object):
    context = {'model': model, 'session': model.Session,
               'user': c.user, 'for_view': True,
               'auth_user_obj': c.userobj}
    try:
        source_package = get_action('package_show')(context, {'id': object})
        return source_package.get('lineage', None)
    except Exception as e:
        abort(404, str(e))

    return ''


def get_relationship_types(field=None):
    types = PackageRelationship.get_all_types()
    return types


def quote_uri(uri):
    from urllib.parse import quote
    if uri is None:
        return ''
    return quote(str(uri), safe='')


def unquote_uri(uri):
    from urllib.parse import unquote
    return unquote(uri)


def get_subject_package_relationship_objects(id):
    try:
        relationships = get_action('subject_package_relationship_objects')({}, {'id': id})
    except Exception as e:
        log.error(str(e))
    relationship_dicts = []
    if relationships:
        try:
            for relationship in relationships:
                if not relationship.object_package_id:
                    relationship_dicts.append(
                        {'subject': id,
                         'type': relationship.type,
                         'object': None,
                         'comment': relationship.comment}
                    )
                else:
                    # Normal CKAN package to package relationship
                    relationship_dicts.append(relationship.as_dict())

            for relationship_dict in relationship_dicts:
                if relationship_dict['object']:
                    # QDES: handle standard CKAN dataset to dataset relationships
                    site_user = get_action(u'get_site_user')({u'ignore_auth': True}, {})
                    context = {u'user': site_user[u'name']}
                    package = get_action('package_show')(context, {'id': relationship_dict['object']})
                    if package:
                        deleted = ' [Deleted]' if package.get('state') == 'deleted' else ''
                        relationship_dict['title'] = package['title'] + deleted
                else:
                    # QDES: handle CKAN dataset to EXTERNAL URI relationships
                    relationship_dict['title'] = relationship_dict['comment']
        except Exception as e:
            log.error(str(e))

    return relationship_dicts


def show_relationships_on_dataset_detail():
    return toolkit.asbool(toolkit.config.get('ckanext.relationships.show_relationships_on_dataset_detail', True))


def build_relationships_nav_icon(pkg_name):
    """
    Build the Relationships tab navigation icon for the dataset view.

    Args:
        pkg_name: The name of the dataset

    Returns:
        HTML for the Relationships tab navigation icon
    """
    from ckan.lib.helpers import build_nav_icon
    return build_nav_icon('relationships.index', _('Relationships'), id=pkg_name, icon='link')
