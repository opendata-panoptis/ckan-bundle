#!/usr/bin/env python3
# encoding: utf-8
#
# This file is part of ckanext-contact
# Created by the Natural History Museum in London, UK

import functools
import os
from logging import getLogger

from ckan.plugins import SingletonPlugin, implements, interfaces, toolkit

from ckanext.contact import routes
from ckanext.contact.auth import send_contact

plugin_dir = os.path.dirname(__file__)

log = getLogger(__name__)


class ContactPlugin(SingletonPlugin):
    """
    CKAN Contact Extension.
    """

    implements(interfaces.IBlueprint, inherit=True)
    implements(interfaces.IConfigurer)
    implements(interfaces.IAuthFunctions)
    implements(interfaces.ITemplateHelpers, inherit=True)
    implements(interfaces.ITranslation)

    ## IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/assets', 'ckanext-contact')

    # ITranslation
    def i18n_directory(self):
        return os.path.join(plugin_dir, 'i18n')

    def i18n_domain(self):
        return 'ckanext-contact'

    def i18n_locales(self):
        return ['el', 'en']

    ## IBlueprint
    def get_blueprint(self):
        return routes.blueprints

    ## IAuthFunctions
    def get_auth_functions(self):
        return {'send_contact': send_contact}

    ## ITemplateHelpers
    def get_helpers(self):
        return {
            'get_recaptcha_v3_action': functools.partial(
                toolkit.config.get, 'ckanext.contact.recaptcha_v3_action', None
            ),
            'get_recaptcha_v3_key': functools.partial(
                toolkit.config.get, 'ckanext.contact.recaptcha_v3_key', None
            ),
        }
