from ckan.plugins import toolkit as tk

_ = tk._
Invalid = tk.Invalid

SHOWCASE_ASSOCIATION_PACKAGE_TYPES = ("dataset", "data-service")


def convert_package_name_or_id_to_id_for_type(package_name_or_id,
                                              context, package_type='dataset'):
    '''
    Return the id for the given package name or id. Only works with packages
    of type package_type.

    Also validates that a package with the given name or id exists.

    :returns: the id of the package with the given name or id
    :rtype: string
    :raises: ckan.lib.navl.dictization_functions.Invalid if there is no
        package with the given name or id

    '''
    session = context['session']
    model = context['model']
    result = session.query(model.Package) \
        .filter_by(id=package_name_or_id, type=package_type).first()
    if not result:
        result = session.query(model.Package) \
            .filter_by(name=package_name_or_id, type=package_type).first()
    if not result:
        raise Invalid('%s: %s' % (_('Not found'), _('Dataset')))
    return result.id


def convert_package_name_or_id_to_id_for_types(package_name_or_id,
                                               context,
                                               package_types=None):
    package_types = tuple(package_types or SHOWCASE_ASSOCIATION_PACKAGE_TYPES)
    result = _get_package_for_types(package_name_or_id, context, package_types)
    if not result:
        raise Invalid('%s: %s' % (_('Not found'), _('Dataset or API')))

    return result.id


def _get_package_for_types(package_name_or_id, context, package_types):
    session = context['session']
    model = context['model']

    query = session.query(model.Package).filter(
        model.Package.type.in_(package_types)
    )
    result = query.filter(model.Package.id == package_name_or_id).first()
    if not result:
        result = query.filter(model.Package.name == package_name_or_id).first()
    return result


def _is_valid_association_target(result):
    is_public = result.private in (False, None)
    is_active = result.state == 'active'

    if result.type == 'dataset':
        return is_public and is_active

    if result.type == 'data-service':
        has_owner_org = bool(result.owner_org)
        return is_public and is_active and has_owner_org

    return False


def convert_package_name_or_id_to_id_for_association_target(
    package_name_or_id,
    context,
):
    package_types = tuple(SHOWCASE_ASSOCIATION_PACKAGE_TYPES)
    result = _get_package_for_types(package_name_or_id, context, package_types)
    if not result:
        raise Invalid('%s: %s' % (_('Not found'), _('Dataset or API')))

    if not _is_valid_association_target(result):
        raise Invalid('%s: %s' % (_('Not found'), _('Dataset or API')))

    return result.id


def convert_package_name_or_id_to_id_for_type_dataset(package_name_or_id,
                                                      context):
    return convert_package_name_or_id_to_id_for_type(package_name_or_id,
                                                     context,
                                                     package_type='dataset')


def convert_package_name_or_id_to_id_for_type_dataset_or_data_service(
    package_name_or_id, context
):
    return convert_package_name_or_id_to_id_for_types(
        package_name_or_id,
        context,
        package_types=SHOWCASE_ASSOCIATION_PACKAGE_TYPES,
    )


def convert_package_name_or_id_to_id_for_association_package(
    package_name_or_id, context
):
    return convert_package_name_or_id_to_id_for_association_target(
        package_name_or_id,
        context,
    )


def convert_package_name_or_id_to_id_for_type_showcase(package_name_or_id,
                                                       context):
    return convert_package_name_or_id_to_id_for_type(package_name_or_id,
                                                     context,
                                                     package_type='showcase')


def showcase_package_type_filter(value, context):
    normalized = (value or '').strip().lower()
    if not normalized or normalized == 'all':
        return None
    if normalized not in SHOWCASE_ASSOCIATION_PACKAGE_TYPES:
        raise Invalid('%s: %s' % (_('Invalid value'), _('Dataset or API')))
    return normalized
