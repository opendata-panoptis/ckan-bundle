import ckan.plugins.toolkit as toolkit
import logging

from ckan.logic import auth_allow_anonymous_access, NotFound
from ckan.model import meta
from ckan.model.package_relationship import PackageRelationship

log = logging.getLogger(__name__)


@auth_allow_anonymous_access
@toolkit.chained_action
def package_relationship_create(original_action, context, data_dict):
    toolkit.check_access('package_update', context, {'id': data_dict.get('subject', None)})
    toolkit.check_access('package_relationship_create', context, data_dict)

    model = context['model']
    object_id = data_dict.get('object', None)
    subject = data_dict.get('subject', None)
    relationship_type = data_dict.get('type', None)

    # Η δημιουργία διασύνδεση θα πρέπε να υποστηρίζεται για dataset
    pkg1 = model.Package.get(subject)

    try:
        validate_is_dataset(pkg1)
    except toolkit.ValidationError as ve:
        raise ve

    pkg2 = None

    if object_id:
        pkg2 = model.Package.get(object_id)

        try:
            validate_is_dataset(pkg2)
        except toolkit.ValidationError as ve:
            raise ve

    # Αν δημιουργουμε διασύνδεση  συνόλου δεδομένων με URI (χωρίς object_id)
    if not object_id:
        comment = data_dict.get('comment', u'') or None  # Just in case it is an empty string


        if not pkg1:
            raise NotFound('>>> Subject package {0} was not found.'.format(subject))

        if comment:
            # Check if a matching external URI relationship already exists
            # otherwise we'd end up creating a duplicate
            existing_relationship = toolkit.get_action('get_package_relationship_by_uri')(context, {
                'id': subject,
                'uri': comment,
                'type': relationship_type,
            })

            if not existing_relationship:
                relationship = PackageRelationship(
                    subject=pkg1,
                    object=None,
                    type=relationship_type,
                    comment=comment)

                meta.Session.add(relationship)
                meta.Session.commit()

            # rel = pkg1.add_relationship(rel_type, pkg2, comment=comment)
            # if not context.get('defer_commit'):
            #     model.repo.commit_and_remove()
            # context['relationship'] = rel

    # else We revert to the parent/CKAN core `package_relationship_create` action
    else:
        # Αν δημιουργούμε διασύνδεση σύνολων δεδομένων
        log.info('*** Reverting to core CKAN package_relationship_create for:')
        log.info(data_dict)
        try:

            if not pkg2:
                raise NotFound('>>> Object package {0} was not found.'.format(object_id))

            result = original_action(context, data_dict)

            # Optional: Commit if not deferred
            if not context.get('defer_commit'):
                model.repo.commit_and_remove()

            log.info('>>> Core relationship created successfully.')
            return result

        except toolkit.Invalid as ex:
            log.warning('>>> Relationship creation failed with Invalid: %s', ex)
            raise

        except Exception as ex:
            log.error('>>> Unexpected error in fallback relationship creation: %s', ex)
            raise

    # return True
    return {'success': True}


from ckan import plugins as p
def validate_is_dataset(pkg):
    if not pkg:
        raise p.toolkit.ValidationError({'package': 'Package not found.'})

    if pkg.type != 'dataset':
        raise toolkit.ValidationError({
            'object_package_id': ['Error message']
        })