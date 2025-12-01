import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.logic as logic
import ckan.model as model

from ckan.controllers.package import PackageController
from ckan.common import c
from ckan.lib.navl.dictization_functions import unflatten


abort = base.abort
get_action = logic.get_action
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
render = base.render
request = base.request
clean_dict = logic.clean_dict
tuplize_dict = logic.tuplize_dict
parse_params = logic.parse_params


class RelationshipController(PackageController):

    def dataset_relationships(self, id):
        context = {'model': model, 'session': model.Session,
                   'user': c.user, 'for_view': True,
                   'auth_user_obj': c.userobj}
        data_dict = {'id': id}

        try:
            c.pkg_dict = get_action('package_show')(context, data_dict)
            c.pkg = context['package']
        except (NotFound, NotAuthorized):
            abort(404, _('Dataset not found'))

        # used by disqus plugin
        c.current_package_id = c.pkg.id

        return render('package/relationships.html')

    def create_dataset_relationship(self, id):

        if request.method == 'POST':
            data_dict = clean_dict(unflatten(tuplize_dict(parse_params(request.POST))))
            # from pprint import pprint
            # pprint(data_dict)
            object = data_dict.get('object', None)
            type = data_dict.get('type', None)
            if object:
                context = {'model': model, 'session': model.Session,
                           'user': c.user, 'for_view': True,
                           'auth_user_obj': c.userobj}

                try:
                    relationship = get_action('package_relationship_create')(context, {
                        'subject': id,
                        'object': object,
                        'type': type
                    })
                    from pprint import pprint
                    pprint(type)
                    # if the relationship type is 'has_derivation' - add the creator of the dataset as a follower of the subject dataset (id)
                    if type == 'derives_from':
                        # TODO: Check if the user is already following

                        following = get_action('follow_dataset')(context, {
                            'id': object
                        })
                        pprint(following)

                except Exception, e:
                    # TODO: Deal with exception raised when adding as child_of:
                    # 'Parent instance <PackageRelationship at 0x7fd118badb10> is not bound to a Session; lazy load operation of attribute 'subject' cannot proceed'
                    print(str(e))

        h.redirect_to(h.url_for('dataset_relationships', id=id))

    def delete_dataset_relationship(self, id, type, object):
        context = {'model': model, 'session': model.Session,
                   'user': c.user, 'for_view': True,
                   'auth_user_obj': c.userobj}

        get_action('package_relationship_delete')(context, {
            'subject': id,
            'object': object,
            'type': type
        })
        h.redirect_to(h.url_for('dataset_relationships', id=id))
