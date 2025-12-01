import ckan.authz as authz
import ckan.plugins.toolkit as toolkit
import logging

from ckan.common import _, config

log = logging.getLogger(__name__)


# @toolkit.chained_auth_function
def package_relationship_create(context, data_dict):
    # Roles allowed to create package relationships regardless
    # of their organisation capacity for the object package
    roles_allowed = config.get('ckanext.relationships.package_relationship_create_ignore_auth_for', None)

    user_name = context.get('user', 'Unknown')

    id = data_dict['subject']

    #
    # @WORKAROUND: Number one
    #
    # CONTEXT:
    #
    # This auth function gets called twice:
    # - once for the subject package in the relationship
    # - then for the object package in the relationship
    #
    # data_dict['subject'] is always the ID of the package being created/updated
    # context.get('package').id is the package being assessed for
    #   package_update permission (could be the object package)
    #
    #
    # On some CKAN instances we may want to allow authorised users to
    # create relationships between packages where they don't have
    # `package_update` permission on the `object` package, but it was still
    # throwing a `NotAuthorized` exception when trying to relax that
    # restriction and reporting that the user did not have `read` access
    # to the subject package that they were updating (i.e. the package they
    # DO have `package_update` permission for.
    #
    if roles_allowed and not toolkit.current_user.is_anonymous:
        # On new datasets the context.get('package') has not been set yet, so need to use package_show for the subject
        # data_dict['subject'] is always the ID of the package being created/updated
        context_package = toolkit.get_action('package_show')(context, {"id": data_dict['subject']})
        for role in roles_allowed.split():
            # I.E. If the user has admin/editor permission for the subject package
            # ignore further auth checks
            if authz.has_user_permission_for_group_or_org(context_package.get('owner_org'), user_name, role):
                context['ignore_auth'] = True
                break

    #
    # @WORKAROUND: Number two - commented out, but left for context
    #
    # Regardless of if the authenticated user has `package_update`
    # permission for the package they are trying to create a relationship
    # on it was failing to give authorisation.
    #
    # In this section of code, we check if they have admin or editor
    # permission for ANY org and if so relax the restrictions by
    # setting `context['ignore_auth'] = True`

    # # 1. Is the user logged in?
    # if roles_allowed \
    #         and authz.auth_is_loggedin_user:
    #     ignore_auth = False
    #     # 2. Does the user have editor or admin capacity for any organisation?
    #     # NOTE: Tried to use `authz.has_user_permission_for_group_or_org` here -
    #     # but this auth function is called for each package in the relationship,
    #     # therefore it failed on the object package if the user was not an
    #     # admin/editor for the object package.owner_org
    #     for role in roles_allowed.split():
    #         if authz.has_user_permission_for_some_org(user_name, role):
    #             ignore_auth = True
    #     if ignore_auth:
    #         context['ignore_auth'] = True

    # Default to the normal behaviour
    authorized = authz.is_authorized_boolean(
        'package_update', context, {'id': id})

    if not authorized:
        return {'success': False, 'msg': _('User %s not authorized to edit this package') % user_name}
    else:
        return {'success': True}
