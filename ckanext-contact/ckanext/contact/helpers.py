from __future__ import annotations

import logging
from typing import Any

import ckan.plugins.toolkit as toolkit

log = logging.getLogger(__name__)


def _get_config_as_bool(key: str, default: bool = False) -> bool:
    """
    Parse a CKAN config option as boolean.

    - If key is missing -> default
    - If value is list (can happen from admin config UI patterns) -> use last item
    - Accept common truthy strings: true/1/yes/on
    """
    value: Any = toolkit.config.get(key, None)

    if value is None:
        return default

    if isinstance(value, list):
        if not value:
            return default
        value = value[-1]

    value_str = str(value).strip()
    if value_str == "":
        return default

    try:
        return toolkit.asbool(value_str)
    except Exception:
        log.warning("Invalid boolean config %s=%r, using default=%r", key, value, default)
        return default


def contact_accept_terms_enabled() -> bool:
    """
    Feature flag for showing the 'accept terms' checkbox block in the contact form.

    Config key:
      - ckanext.contact.accept_terms.enabled

    Default:
      - False (if not declared in ckan.ini)
    """
    return _get_config_as_bool("ckanext.contact.accept_terms.enabled", default=False)


def contact_organization_field_enabled() -> bool:
    """
    Feature flag for showing the 'Organization' select field in the contact form.

    Config key:
      - ckanext.contact.organization_field.enabled

    Default:
      - False (hidden if not declared)
    """
    return _get_config_as_bool("ckanext.contact.organization_field.enabled", default=False)


def contact_support_faq_enabled() -> bool:
    """
    Feature flag for showing the support FAQ section in the contact page.

    Config key:
      - ckanext.contact.support_faq.enabled

    Default:
      - False (hidden if not declared)
    """
    return _get_config_as_bool("ckanext.contact.support_faq.enabled", default=False)


def contact_support_tree_enabled() -> bool:
    """
    Feature flag for showing the support-tree section in the contact page
    AND the related subject-type field in the contact form.

    Config key:
      - ckanext.contact.support_tree.enabled

    Default:
      - False (disabled by default)
    """
    return _get_config_as_bool("ckanext.contact.support_tree.enabled", default=False)


def get_helpers():
    return {
        "contact_accept_terms_enabled": contact_accept_terms_enabled,
        "contact_organization_field_enabled": contact_organization_field_enabled,
        "contact_support_faq_enabled": contact_support_faq_enabled,
        "contact_support_tree_enabled": contact_support_tree_enabled,
    }