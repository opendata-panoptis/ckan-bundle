# -*- coding: utf-8 -*-

from functools import wraps
import os
import json

from ckantoolkit import config

from ckan import plugins as p

from ckan.lib.plugins import DefaultTranslation

import ckanext.dcat.blueprints as blueprints
import ckanext.dcat.cli as cli

from ckanext.dcat.logic import (dcat_dataset_show,
                                dcat_catalog_show,
                                dcat_catalog_search,
                                dcat_datasets_list,
                                dcat_auth,
                                )
from ckanext.dcat import helpers
from ckanext.dcat import utils
from ckanext.dcat.validators import dcat_validators


CUSTOM_ENDPOINT_CONFIG = 'ckanext.dcat.catalog_endpoint'
TRANSLATE_KEYS_CONFIG = 'ckanext.dcat.translate_keys'

HERE = os.path.abspath(os.path.dirname(__file__))
I18N_DIR = os.path.join(HERE, u"../i18n")


def config_declaration(arg=None):
    supports_config_declaration = p.toolkit.check_ckan_version(min_version="2.10.0")

    # @config_declaration with no args
    if callable(arg):
        if supports_config_declaration:
            return p.toolkit.blanket.config_declarations(arg)
        return arg

    # @config_declaration with custom file
    def decorator(cls):
        if supports_config_declaration:
            return p.toolkit.blanket.config_declarations(arg)(cls)
        return cls

    return decorator


def _get_dataset_schema(dataset_type="dataset"):
    schema = None
    try:
        schema_show = p.toolkit.get_action("scheming_dataset_schema_show")
        try:
            schema = schema_show({}, {"type": dataset_type})
        except p.toolkit.ObjectNotFound:
            pass
    except KeyError:
        pass
    return schema


@config_declaration
class DCATPlugin(p.SingletonPlugin, DefaultTranslation):

    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.ITemplateHelpers, inherit=True)
    p.implements(p.IActions, inherit=True)
    p.implements(p.IAuthFunctions, inherit=True)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITranslation, inherit=True)
    p.implements(p.IClick)
    p.implements(p.IBlueprint)
    p.implements(p.IValidators)

    # IClick

    def get_commands(self):
        return cli.get_commands()

    # IBlueprint

    def get_blueprint(self):
        return [blueprints.dcat]

    # ITranslation

    def i18n_directory(self):
        return I18N_DIR

    # IConfigurer

    def update_config(self, config):
        p.toolkit.add_template_directory(config, '../templates/dcat')

        # Check catalog URI on startup to emit a warning if necessary
        utils.catalog_uri()

        # Check custom catalog endpoint
        custom_endpoint = config.get(CUSTOM_ENDPOINT_CONFIG)
        if custom_endpoint:
            if not custom_endpoint[:1] == '/':
                raise Exception(
                    '"{0}" should start with a backslash (/)'.format(
                        CUSTOM_ENDPOINT_CONFIG))
            if '{_format}' not in custom_endpoint:
                raise Exception(
                    '"{0}" should contain {{_format}}'.format(
                        CUSTOM_ENDPOINT_CONFIG))

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'dcat_get_endpoint': helpers.get_endpoint,
            'dcat_endpoints_enabled': helpers.endpoints_enabled,
        }

    # IActions

    def get_actions(self):
        return {
            'dcat_dataset_show': dcat_dataset_show,
            'dcat_catalog_show': dcat_catalog_show,
            'dcat_catalog_search': dcat_catalog_search,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'dcat_dataset_show': dcat_auth,
            'dcat_catalog_show': dcat_auth,
            'dcat_catalog_search': dcat_auth,
        }

    # IValidators
    def get_validators(self):
        return dcat_validators

    # IPackageController

    # CKAN < 2.10 hooks
    def after_show(self, context, data_dict):
        return self.after_dataset_show(context, data_dict)

    def before_index(self, dataset_dict):
        return self.before_dataset_index(dataset_dict)

    # CKAN >= 2.10 hooks
    def after_dataset_show(self, context, data_dict):

        schema = _get_dataset_schema(data_dict["type"])
        # check if config is enabled to translate keys (default: True)
        # skip if scheming is enabled, as this will be handled there
        translate_keys = (
            p.toolkit.asbool(config.get(TRANSLATE_KEYS_CONFIG, True))
            and not schema
        )

        if not translate_keys:
            return data_dict

        if context.get('for_view'):
            field_labels = utils.field_labels()

            def set_titles(object_dict):
                for key, value in object_dict.copy().items():
                    if key in field_labels:
                        object_dict[field_labels[key]] = object_dict[key]
                        del object_dict[key]

            for resource in data_dict.get('resources', []):
                set_titles(resource)

            for extra in data_dict.get('extras', []):
                if extra['key'] in field_labels:
                    extra['key'] = field_labels[extra['key']]

        return data_dict

    def before_dataset_index(self, dataset_dict):
        schema = _get_dataset_schema(dataset_dict["type"])
        spatial = None
        if schema:
            for field in schema['dataset_fields']:
                if field['field_name'] in dataset_dict and 'repeating_subfields' in field:
                    # Check value because of ckan/ckan#8953
                    value = dataset_dict[field['field_name']]
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except ValueError:
                            continue

                    for item in value:
                        for key in item:
                            value = item[key]
                            if not isinstance(value, dict):
                                # Index a flattened version
                                new_key = f'extras_{field["field_name"]}__{key}'
                                if not dataset_dict.get(new_key):
                                    dataset_dict[new_key] = str(value)
                                else:
                                    dataset_dict[new_key] += ' ' + str(value)

                    subfields = dataset_dict.pop(field['field_name'], None)
                    if field['field_name'] == 'spatial_coverage':
                        spatial = subfields

        # Συνάρτηση που ελέγχει αν υπάρχει κάποιο έγκυρο γεωμετρικό αντικείμενο (σε GeoJSON μορφή)
        def _check_for_a_geom(spatial_dict):

            # Αναζητούμε πιθανά πεδία που μπορεί να περιέχουν γεωμετρία μέσα στο spatial_dict
            for field in ('geom', 'bbox', 'centroid', 'Geometry', 'Bounding Box', 'Centroid'):
                value = spatial_dict.get(field)
                if value:
                    # Αν η τιμή είναι dictionary (π.χ. ένα GeoJSON object), την μετατρέπουμε σε συμβολοσειρά
                    value = spatial_dict[field]
                    if isinstance(value, dict):
                        try:
                            return json.dumps(value)
                        except ValueError:
                            continue
                    # Αν είναι ήδη συμβολοσειρά, ελέγχουμε αν είναι έγκυρο JSON
                    elif isinstance(value, str):
                        try:
                            json.loads(value)
                            return value
                        except ValueError:
                            continue
            # Αν δεν βρεθεί έγκυρη γεωμετρία, επιστρέφουμε None
            return None

        # Ελέγχουμε αν υπάρχουν spatial δεδομένα στο dataset
        if spatial:
            # Αν το spatial είναι string, προσπαθούμε να το μετατρέψουμε σε Python αντικείμενο (λίστα από dicts)
            if isinstance(spatial, str):
                try:
                    spatial = json.loads(spatial)
                except ValueError:
                    spatial = []

            # Διατρέχουμε κάθε αντικείμενο (dict) στον πίνακα spatial
            for item in spatial:
                # Κάνουμε έλεγχο για την πρώτη διαθέσιμη γεωμετρία στο αντικείμενο
                geojson_string = _check_for_a_geom(item)
                if geojson_string:
                    # Αν βρέθηκε έγκυρη γεωμετρία, την καταχωρούμε στα indexable πεδία
                    dataset_dict['spatial'] = geojson_string
                    dataset_dict['extras_spatial'] = geojson_string

                    try:
                        # Κάνουμε parse το GeoJSON string
                        geojson = json.loads(geojson_string)

                        # Αν είναι τύπου "Polygon", εξάγουμε τις συντεταγμένες για υπολογισμό bounding box
                        if geojson.get("type") == "Polygon":
                            coords = geojson.get("coordinates", [])
                            if coords and coords[0]:
                                # Παίρνουμε όλα τα γεωγραφικά πλάτη και μήκη
                                lons = [pt[0] for pt in coords[0]]
                                lats = [pt[1] for pt in coords[0]]

                                # Υπολογίζουμε min/max τιμές για να δημιουργήσουμε bounding box (extents)
                                dataset_dict['minx'] = min(lons)
                                dataset_dict['maxx'] = max(lons)
                                dataset_dict['miny'] = min(lats)
                                dataset_dict['maxy'] = max(lats)

                    except Exception:
                        # Σε περίπτωση σφάλματος (π.χ. κακό format), συνεχίζουμε χωρίς να διακόψουμε τη ροή
                        pass
                    # Μόλις βρεθεί και καταχωρηθεί η πρώτη έγκυρη γεωμετρία, σταματάμε την αναζήτηση
                    break
        # Επιστρέφουμε το τελικό dataset_dict για index στο CKAN
        return dataset_dict


class DCATJSONInterface(p.SingletonPlugin):
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions, inherit=True)
    p.implements(p.IBlueprint)

    # IBlueprint

    def get_blueprint(self):
        return [blueprints.dcat_json_interface]

    # IActions

    def get_actions(self):
        return {
            'dcat_datasets_list': dcat_datasets_list,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'dcat_datasets_list': dcat_auth,
        }


@config_declaration("config_declaration_structured_data.yml")
class StructuredDataPlugin(p.SingletonPlugin):

    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.ITemplateHelpers, inherit=True)

    # IConfigurer

    def update_config(self, config):
        p.toolkit.add_template_directory(config, '../templates/structured_data')

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'structured_data': helpers.structured_data,
        }


@config_declaration("config_declaration_croissant.yml")
class CroissantPlugin(p.SingletonPlugin):

    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.ITemplateHelpers, inherit=True)
    p.implements(p.IBlueprint)

    # IConfigurer

    def update_config(self, config):
        p.toolkit.add_template_directory(config, '../templates/croissant')

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'croissant': helpers.croissant,
        }

    # IBlueprint

    def get_blueprint(self):
        return [blueprints.croissant]
