import ckan.model as model
import ckan.plugins.toolkit as toolkit
import logging

from ckan.model.package_relationship import PackageRelationship

log = logging.getLogger(__name__)


@toolkit.chained_action
def package_relationships_list(original_action, context, data_dict):
    toolkit.check_access('package_relationships_list', context, data_dict)

    relationship_dicts = []

    model = context['model']
    id = data_dict.get('id', None)

    if id:
        try:
            pkg_obj = model.Package.get(id)

            # ref.: CKAN_CORE/ckan/model/package.py as to how this works
            relationships = pkg_obj.get_relationships()

            if relationships:
                # Implement our own `package_relationship.as_dict` method because our
                # relationships may not have an `object_package_id` to get the name from
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
                        relationship_dicts.append(relationship.as_dict(pkg_obj))
            else:
                log.debug('>>> NO Relationships found for package ID: {0} <<<'.format(id))
        except Exception as e:
            log.error(str(e))

    return relationship_dicts


def package_relationship_by_uri(context, data_dict):
    """
    Get a `package_relationship` record based on:
    - package ID
    - URI (if it exists as the package_relationship.comment)
    - relationship type (optional)
    :param context:
    :param data_dict:
    :return:
    """
    pkg_id = data_dict.get('id', None) or None  # Just in case the data_dict values are empty strings
    uri = data_dict.get('uri', None) or None
    relationship_type = data_dict.get('type', None) or None

    if pkg_id and uri:
        try:
            query = model.Session.query(PackageRelationship)
            query = query.filter(PackageRelationship.subject_package_id == pkg_id)
            query = query.filter(PackageRelationship.state == 'active')
            query = query.filter(PackageRelationship.comment == uri)

            if relationship_type:
                query = query.filter(PackageRelationship.type == relationship_type)

            return query.all()
        except Exception as e:
            log.error(str(e))


def subject_package_relationship_objects(context, data_dict):
    """
    Get `package_relationship` records where the package.id provided
    is the "subject" of the relationship AND the relationship state is active
    :param context:
    :param data_dict:
    :return:
    """
    relationship_objects = []

    pkg_id = data_dict.get('id', None)

    if pkg_id:
        try:
            query = model.Session.query(PackageRelationship)
            query = query.filter(PackageRelationship.subject_package_id == pkg_id)
            query = query.filter(PackageRelationship.state == 'active')
            relationship_objects = query.all()
        except Exception as e:
            log.error(str(e))

    return relationship_objects
