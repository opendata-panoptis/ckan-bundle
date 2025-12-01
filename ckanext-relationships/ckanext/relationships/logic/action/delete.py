import ckan.plugins.toolkit as toolkit
import logging
import ckan.lib.dictization as d
from ckan.model import meta, PackageRelationship

log = logging.getLogger(__name__)


def package_relationship_delete_by_uri(context, data_dict):
    toolkit.check_access('package_update', context, {'id': data_dict.get('subject', None)})
    subject = data_dict.get('subject', None)
    type = data_dict.get('type', None)
    uri = data_dict.get('uri', None)

    if subject and type and uri:
        try:
            package_dict = toolkit.get_action('package_show')(context, {'id': subject})

            if package_dict:
                query = meta.Session.query(PackageRelationship)
                query = query.filter(PackageRelationship.subject_package_id == package_dict['id'])
                query = query.filter(PackageRelationship.comment == uri)
                query = query.filter(PackageRelationship.type == type)
                relationship = query.first()

                if relationship:
                    relationship.delete()
                    meta.Session.commit()
                else:
                    log.info('*** No relationship found matching:')
                    log.info(data_dict)
        except Exception as e:
            log.error('*** ERROR: package_relationship_delete_by_uri')
            log.error(str(e))

    return True


def package_relationship_delete_all(context, data_dict):
    toolkit.check_access('package_update', context, {'id': data_dict.get('subject', None)})
    dataset_id = data_dict.get('id', None)
    if dataset_id:
        try:
            model = context['model']
            query = model.Session.query(PackageRelationship)
            query = query.filter(PackageRelationship.subject_package_id == dataset_id)
            relationships = query.all()

            for relationship in relationships:
                log.debug('Deleting relationship : {}'.format(d.table_dictize(relationship, context)))
                relationship.purge()

            model.Session.commit()

        except Exception as e:
            log.error('*** ERROR: package_relationship_delete_all')
            log.error(str(e))
