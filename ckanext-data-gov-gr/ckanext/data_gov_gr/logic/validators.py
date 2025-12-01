import json
import re
import logging
from typing import Any, Dict

import six

from ckan.common import _
from ckan.logic.validators import missing
from ckan.model import PACKAGE_NAME_MAX_LENGTH
from ckan.plugins import toolkit
from ckanext.fluent.helpers import fluent_alternate_languages, fluent_form_languages
from ckanext.fluent.validators import BCP_47_LANGUAGE
from ckanext.scheming.validation import scheming_validator

log = logging.getLogger(__name__)

def custom_name_validator(value: Any, context: Dict) -> Any:
    if not isinstance(value, str):
        raise toolkit.Invalid(_('Names must be strings'))

    # Έλεγχος για όλα τα απαραίτητα keys ταυτόχρονα, για να διαπιστώσουμε αν ελέγχεται user object
    required_user_keys = {'id', 'name', 'fullname', 'password', 'email', 'sysadmin'}
    schema_keys = set(context.get('schema_keys', []))

    is_user_validation = required_user_keys.issubset(schema_keys)

    # Έλεγχος αν επιτρέπεται οποιοσδήποτε χαρακτήρας π.χ. @ από το ckan.ini
    allow_any_character = toolkit.asbool(toolkit.config.get('ckanext.data_gov_gr.user.allow_any_character_in_username', False))

    # Διαφορετικό pattern για users vs άλλες οντότητες
    if is_user_validation and allow_any_character:
        name_match = re.compile(r'^[a-z0-9_\-.@]*$')
        name_match = re.compile(r"^[^\s]{3,255}$")
    else:
        name_match = re.compile(r'^[a-z0-9_\-]*$')

    # Οι υπόλοιποι έλεγχοι παραμένουν ίδιοι από το validators.py του ckan
    if value in ['new', 'edit', 'search']:
        raise toolkit.Invalid(_('That name cannot be used'))

    if len(value) < 2:
        raise toolkit.Invalid(_('Must be at least %s characters long') % 2)

    if len(value) > PACKAGE_NAME_MAX_LENGTH:
        raise toolkit.Invalid(_('Name must be a maximum of %i characters long')
                              % PACKAGE_NAME_MAX_LENGTH)

    if not name_match.match(value):
        raise toolkit.Invalid(_('Must be purely lowercase alphanumeric '
                                '(ascii) characters and these symbols: -_@'))
    return value

def get_validators():
    return {
        'name_validator': custom_name_validator ,  # Αντικαθιστά τον default name_validator
        'custom_fluent_text': custom_fluent_text
    }

@scheming_validator
def custom_fluent_text(field, schema):
    """
    Accept multilingual text input in the following forms
    and convert to a json string for storage:

    1. a multilingual dict, eg.

       {"en": "Text", "fr": "texte"}

    2. a JSON encoded version of a multilingual dict, for
       compatibility with old ways of loading data, eg.

       '{"en": "Text", "fr": "texte"}'

    3. separate fields per language (for form submissions):

       fieldname-en = "Text"
       fieldname-fr = "texte"

    When using this validator in a ckanext-scheming schema setting
    "required" to true will make all form languages required to
    pass validation.
    """
    # combining scheming required checks and fluent field processing
    # into a single validator makes this validator more complicated,
    # but should be easier for fluent users and eliminates quite a
    # bit of duplication in handling the different types of input
    required_langs = []
    alternate_langs = {}
    if field and field.get('required'):
        required_langs = field.get('required_languages') or fluent_form_languages(field, schema=schema)
        alternate_langs = fluent_alternate_languages(field, schema=schema)

    def validator(key, data, errors, context):
        # just in case there was an error before our validator,
        # bail out here because our errors won't be useful
        if errors[key]:
            return

        value = data[key]
        # 1 or 2. dict or JSON encoded string
        if value is not missing:
            if isinstance(value, six.string_types):
                try:
                    value = json.loads(value)
                except ValueError:
                    errors[key].append(_('Failed to decode JSON string'))
                    return
                except UnicodeDecodeError:
                    errors[key].append(_('Invalid encoding for JSON string'))
                    return
            if not isinstance(value, dict):
                errors[key].append(_('expecting JSON object'))
                return

            for lang, text in value.items():
                try:
                    m = re.match(BCP_47_LANGUAGE, lang)
                except TypeError:
                    errors[key].append(_('invalid type for language code: %r')
                        % lang)
                    continue
                if not m:
                    errors[key].append(_('invalid language code: "%s"') % lang)
                    continue
                if not isinstance(text, six.string_types):
                    errors[key].append(_('invalid type for "%s" value') % lang)
                    continue
                if isinstance(text, str):
                    try:
                        value[lang] = text if six.PY3 else text.decode(
                            'utf-8')
                    except UnicodeDecodeError:
                        errors[key]. append(_('invalid encoding for "%s" value')
                            % lang)

            for lang in required_langs:
                if value.get(lang) or any(
                        value.get(l) for l in alternate_langs.get(lang, [])):
                    continue
                errors[key].append(_('Required language "%s" missing') % lang)

            if not errors[key]:
                data[key] = json.dumps(value, ensure_ascii=False)
            return

        # 3. separate fields
        output = {}
        prefix = key[-1] + '-'
        extras = data.get(key[:-1] + ('__extras',), {})

        for name, text in extras.items():
            if not name.startswith(prefix):
                continue
            lang = name.split('-', 1)[1]
            m = re.match(BCP_47_LANGUAGE, lang)
            if not m:
                errors[name] = [_('invalid language code: "%s"') % lang]
                output = None
                continue

            if output is not None:
                output[lang] = text

        for lang in required_langs:
            if extras.get(prefix + lang) or any(
                    extras.get(prefix + l) for l in alternate_langs.get(lang, [])):
                continue
            errors[key[:-1] + (key[-1] + '-' + lang,)] = [_('Missing value')]
            output = None

        if output is None:
            return

        for lang in output:
            del extras[prefix + lang]
        data[key] = json.dumps(output, ensure_ascii=False)

    return validator

