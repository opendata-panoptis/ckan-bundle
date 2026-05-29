import ckan.plugins as p
import ckan.model as model
from ckan.common import request, _
from markupsafe import Markup, escape


def group_tree(organizations=[], type_='organization'):
    full_tree_list = p.toolkit.get_action('group_tree')({}, {'type': type_})

    if not organizations:
        return full_tree_list
    else:
        filtered_tree_list = group_tree_filter(organizations, full_tree_list)
        return filtered_tree_list


def group_tree_filter(organizations, group_tree_list, highlight=False):
    # this method leaves only the sections of the tree corresponding to the
    # list since it was developed for the users, all children organizations
    # from the organizations in the list are included
    def traverse_select_highlighted(group_tree, selection=[], highlight=False):
        # add highlighted branches to the filtered tree
        if group_tree['highlighted']:
            # add to the selection and remove highlighting if necessary
            if highlight:
                selection += [group_tree]
            else:
                selection += group_tree_highlight([], [group_tree])
        else:
            # check if there is any highlighted child tree
            for child in group_tree.get('children', []):
                traverse_select_highlighted(child, selection)

    filtered_tree = []
    # first highlights all the organizations from the list in the three
    for group in group_tree_highlight(organizations, group_tree_list):
        traverse_select_highlighted(group, filtered_tree, highlight)

    return filtered_tree


def group_tree_section(id_, type_='organization', include_parents=True,
                       include_siblings=True):
    return p.toolkit.get_action('group_tree_section')(
        {'include_parents': include_parents,
         'include_siblings': include_siblings},
        {'id': id_, 'type': type_, })


def _get_action_name(group_id):
    model_obj = model.Group.get(group_id)
    return "organization_show" if model_obj.is_organization else "group_show"


def group_tree_parents(id_):
    action_name = _get_action_name(id_)
    data_dict = {
        'id': id_,
        'include_dataset_count': False,
        'include_users': False,
        'include_followers': False,
        'include_tags': False
    }
    tree_node = p.toolkit.get_action(action_name)({}, data_dict)
    if (tree_node['groups']):
        parent_id = tree_node['groups'][0]['name']
        parent_node = \
            p.toolkit.get_action(action_name)({}, {'id': parent_id})
        return group_tree_parents(parent_id) + [parent_node]
    else:
        return []


def group_tree_get_longname(id_, default="", type_='organization'):
    action_name = _get_action_name(id_)
    data_dict = {
        'id': id_,
        'include_dataset_count': False,
        'include_users': False,
        'include_followers': False,
        'include_tags': False
    }
    tree_node = p.toolkit.get_action(action_name)({}, data_dict)
    longname = tree_node.get("longname", default)
    if not longname:
        return default
    return longname


def group_tree_highlight(organizations, group_tree_list):
    def traverse_highlight(group_tree, name_list):
        if group_tree.get('name', "") in name_list:
            group_tree['highlighted'] = True
        else:
            group_tree['highlighted'] = False
        for child in group_tree.get('children', []):
            traverse_highlight(child, name_list)

    selected_names = [o.get('name', None) for o in organizations]

    for group in group_tree_list:
        traverse_highlight(group, selected_names)
    return group_tree_list


def get_allowable_parent_groups(group_id):
    if group_id:
        group = model.Group.get(group_id)
        allowable_parent_groups = \
            group.groups_allowed_to_be_its_parent(type=group.type)
    else:
        allowable_parent_groups = model.Group.all(
            group_type=p.toolkit.get_endpoint()[0])
    return allowable_parent_groups


def is_include_children_selected():
    include_children_selected = False

    if p.toolkit.check_ckan_version(min_version="2.10"):
        is_flask = True
    else:
        from ckan.common import is_flask_request
        is_flask = is_flask_request()

    if is_flask:
        if request.args.get('include_children'):
            include_children_selected = True
    return include_children_selected


def _collect_node_ids(nodes):
    ids = []
    for node in nodes:
        ids.append(node['id'])
        if node.get('children'):
            ids.extend(_collect_node_ids(node['children']))
    return ids


def _bulk_get_longnames(group_ids):
    if not group_ids:
        return {}
    from ckan.model.group_extra import group_extra_table
    from sqlalchemy import select as sa_select
    result = model.Session.execute(
        sa_select(
            group_extra_table.c.group_id,
            group_extra_table.c.value
        ).where(
            group_extra_table.c.group_id.in_(group_ids),
        ).where(
            group_extra_table.c.key == 'longname',
        ).where(
            group_extra_table.c.state == 'active',
        )
    )
    return {row.group_id: row.value for row in result if row.value}


def _render_nodes(parts, nodes, longnames, base_url, type_,
                  use_longnames, associated_ids, is_top=False):
    css_class = 'hierarchy-tree-top' if is_top else 'hierarchy-tree'
    parts.append(u'<ul class="{}">'.format(css_class))

    for node in nodes:
        name = node['name']
        title = node['title']
        node_id = node['id']
        highlighted = node.get('highlighted', False)

        if use_longnames:
            longname = longnames.get(node_id, '')
            if longname:
                display_text = u'{} ({})'.format(longname, title)
            else:
                display_text = title
        else:
            display_text = title

        li_class = u' class="highlighted"' if highlighted else u''
        parts.append(u'<li{} id="node_{}">'.format(li_class, escape(name)))

        url = base_url.replace('__placeholder__', name)

        if type_ == 'group':
            parts.append(u'<div class="node-item">')
            parts.append(u'<a href="{}">{}</a>'.format(
                escape(url), escape(display_text)))
            if node_id in associated_ids:
                parts.append(u' {}'.format(escape(_('(associated)'))))
                parts.append(
                    u'<input name="group_remove.{}" value="{}" '
                    u'type="submit" class="btn btn-danger btn-sm media-edit" '
                    u'title="{}"/>'.format(
                        escape(name),
                        escape(_('Remove')),
                        escape(_('Remove dataset from this group'))))
            parts.append(u'</div>')
        else:
            parts.append(u'<a href="{}">{}</a>'.format(
                escape(url), escape(display_text)))

        if node.get('children'):
            _render_nodes(parts, node['children'], longnames, base_url,
                          type_, use_longnames, associated_ids)

        parts.append(u'</li>')

    parts.append(u'</ul>')


def render_tree_html(top_nodes, type_='organization', use_longnames=False,
                     use_shortnames=False, pkg_dict=None):
    longnames = {}
    if use_longnames:
        all_ids = _collect_node_ids(top_nodes)
        longnames = _bulk_get_longnames(all_ids)

    base_url = p.toolkit.url_for(
        '{}.read'.format(type_), id='__placeholder__')

    associated_ids = set()
    if pkg_dict and pkg_dict.get('groups'):
        for g in pkg_dict['groups']:
            if g.get('user_member'):
                associated_ids.add(g['id'])

    parts = []
    _render_nodes(parts, top_nodes, longnames, base_url, type_,
                  use_longnames, associated_ids, is_top=True)

    return Markup(u''.join(parts))
