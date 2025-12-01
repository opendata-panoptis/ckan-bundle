import logging

from ckan.model.package import Package
from ckan.model import meta

log = logging.getLogger(__name__)


def package_get(reference, for_update=False):
    '''Returns a package object referenced by its id or name.'''

    if not reference:
        # Find the callers method name
        # If it is from 'index_package' return a new initialised package object to prevent a error of trying to retrieve the name attribute from a None object
        # qdes_ckan-29_lagoon/ckan/ckan_core/ckan/lib/search/index.py Line 224 rel_dict[type].append(model.Package.get(rel['object_package_id']).name)
        import inspect
        calling_method = ''
        try:
            # TODO: Need to investigate potentially a better way of finding key names instead of using indexes
            calling_method = inspect.stack()[1][3]
        except Exception as e:
            log.error('qdes_override_package_get - inspect.stack: {0}'.format(e))

        if calling_method == 'index_package':
            return Package()
        else:
            return None

    q = meta.Session.query(Package)
    if for_update:
        q = q.with_for_update()
    pkg = q.get(reference)
    if pkg == None:
        pkg = Package.by_name(reference, for_update=for_update)
    return pkg

def package_relationship_as_dict(self, package=None, ref_package_by='id'):
    """Returns full relationship info as a dict from the point of view
    of the given package if specified.
    e.g. {'subject':u'annakarenina',
            'type':u'depends_on',
            'object':u'warandpeace',
            'comment':u'Since 1843'}"""
    subject_pkg = self.subject
    object_pkg = self.object
    relationship_type = self.type
    if package and package == object_pkg:
        subject_pkg = self.object
        object_pkg = self.subject
        relationship_type = self.forward_to_reverse_type(self.type)
    subject_ref = getattr(subject_pkg, ref_package_by)
    object_ref = getattr(object_pkg, ref_package_by) if object_pkg else None    
    return {'subject':subject_ref,
            'type':relationship_type,
            'object':object_ref,
            'comment':self.comment}
