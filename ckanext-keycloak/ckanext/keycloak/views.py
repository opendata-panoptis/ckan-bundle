import logging
from flask import Blueprint, session
from ckan.plugins import toolkit as tk
import ckan.lib.helpers as h
import ckan.model as model
from ckan.common import g
from ckan.views.user import set_repoze_user, RequestResetView, login as original_login, logout as original_logout
from ckanext.keycloak.keycloak import KeycloakClient
import ckanext.keycloak.helpers as helpers
from os import environ
import secrets

log = logging.getLogger(__name__)

keycloak = Blueprint('keycloak', __name__, url_prefix='/user')


server_url = tk.config.get('ckanext.keycloak.server_url', environ.get('CKANEXT__KEYCLOAK__SERVER_URL'))
client_id = tk.config.get('ckanext.keycloak.client_id', environ.get('CKANEXT__KEYCLOAK__CLIENT_ID'))
realm_name = tk.config.get('ckanext.keycloak.realm_name', environ.get('CKANEXT__KEYCLOAK__REALM_NAME'))
redirect_uri = tk.config.get('ckanext.keycloak.redirect_uri', environ.get('CKANEXT__KEYCLOAK__REDIRECT_URI'))
client_secret_key = tk.config.get('ckanext.keycloak.client_secret_key', environ.get('CKANEXT__KEYCLOAK__CLIENT_SECRET_KEY'))
allow_any_character = tk.asbool(tk.config.get('ckanext.data_gov_gr.user.allow_any_character_in_username', False))
use_sub_as_name = tk.asbool(tk.config.get('ckanext.keycloak.use_sub_as_name', False))
enable_state_check = tk.asbool(tk.config.get('ckanext.keycloak.enable_state_check', True))

client = KeycloakClient(server_url, client_id, realm_name, client_secret_key)

def _log_user_into_ckan(resp):
    """ Log the user into different CKAN versions.
    CKAN 2.10 introduces flask-login and login_user method.
    CKAN 2.9.6 added a security change and identifies the user
    with the internal id plus a serial autoincrement (currently static).
    CKAN <= 2.9.5 identifies the user only using the internal id.
    """
    if tk.check_ckan_version(min_version="2.10"):
        from ckan.common import login_user
        login_user(g.user_obj)
        return

    if tk.check_ckan_version(min_version="2.9.6"):
        user_id = "{},1".format(g.user_obj.id)
    else:
        user_id = g.user
    set_repoze_user(user_id, resp)

    log.info(u'User {0}<{1}> logged in successfully'.format(g.user_obj.name, g.user_obj.email))

def sso():
    log.info("SSO Login")

    state = secrets.token_urlsafe(32)
    if enable_state_check:
        session["oidc_state"] = state

    auth_url = None
    try:
        max_age_str = tk.config.get('ckanext.keycloak.max_age', environ.get('CKANEXT__KEYCLOAK__MAX_AGE'))
        prompt = tk.config.get('ckanext.keycloak.prompt', environ.get('CKANEXT__KEYCLOAK__PROMPT'))

        # Μετατροπή max_age σε integer αν υπάρχει
        max_age = None
        if max_age_str:
            max_age = int(max_age_str)

        auth_url = client.get_auth_url(
            redirect_uri=redirect_uri,
            max_age=max_age,
            prompt=prompt,
            state=state if enable_state_check else None
        )

        log.info(f"Auth URL generated with max_age={max_age}, prompt={prompt}")

    except Exception as e:
        log.error("Error getting auth url: {}".format(e))
        return tk.abort(500, "Error getting auth url: {}".format(e))
    return tk.redirect_to(auth_url)

def sso_login():

    # Ελέγχουμε το state μόνο αν είναι ενεργοποιημένος ο έλεγχος
    if enable_state_check:
        returned_state = tk.request.args.get("state")
        original_state = session.pop("oidc_state", None)

        if not returned_state or returned_state != original_state:
            log.error("OIDC state mismatch")
            h.flash_error("Σφάλμα OIDC state mismatch στη σύνδεση. Παρακαλώ ξαναπροσπαθήστε.")
            return tk.redirect_to(tk.url_for('user.login'))
    else:
        log.info("OIDC state check is disabled - skipping state validation")

    data = tk.request.args
    token = client.get_token(data['code'], redirect_uri)
    session['keycloak_id_token'] = token.get('id_token')
    userinfo = client.get_user_info(token)
    log.info("SSO Login: {}".format(userinfo))
    if userinfo:
        # Επιλέγουμε το username βάσει config
        if use_sub_as_name:
            username = 'user_' + userinfo['sub']
        else:
            username = userinfo['preferred_username']

        user_dict = {
            'name': username,
            'email': userinfo.get('email', userinfo['sub']+'@localhost.gr'),
            'password': helpers.generate_password(),
            'fullname': userinfo['name'],
            'plugin_extras': {
                'idp': 'google',
                'sub': userinfo['sub']
            }
        }
        context = {"model": model, "session": model.Session}

        try:
            g.user_obj = helpers.process_user(user_dict)
            g.user = g.user_obj.name
            context['user'] = g.user
            context['auth_user_obj'] = g.user_obj

            response = tk.redirect_to(tk.url_for('user.me', context))

            _log_user_into_ckan(response)
            log.info("Logged in success")
            return response

        except tk.ValidationError as e:
            log.error(f"ValidationError during SSO login process: {e}")
            # Εξάγουμε το πρώτο μήνυμα από το ValidationError
            if hasattr(e, 'error_dict') and e.error_dict:
                # Παίρνουμε το πρώτο error message από το error_dict
                first_key = next(iter(e.error_dict))
                error_messages = e.error_dict[first_key]
                if error_messages and len(error_messages) > 0:
                    error_message = error_messages[0]
                else:
                    error_message = 'Παρουσιάστηκε πρόβλημα κατά τη σύνδεση. Παρακαλώ δοκιμάστε ξανά.'
            else:
                error_message = 'Παρουσιάστηκε πρόβλημα κατά τη σύνδεση. Παρακαλώ δοκιμάστε ξανά.'

            h.flash_error(error_message)
            return tk.redirect_to(tk.url_for('user.login'))

        except Exception as e:
            log.error(f"Error during SSO login process: {e}")
            h.flash_error('Παρουσιάστηκε πρόβλημα κατά τη σύνδεση. Παρακαλώ δοκιμάστε ξανά.')
            return tk.redirect_to(tk.url_for('user.login'))

    else:
        return tk.redirect_to(tk.url_for('user.login'))

def reset_password():
    email = tk.request.form.get('user', None)
    if '@' not in email:
        log.info(f'User requested reset link for invalid email: {email}')
        h.flash_error('Invalid email address')
        return tk.redirect_to(tk.url_for('user.request_reset'))
    user = model.User.by_email(email)
    if not user:
        log.info(u'User requested reset link for unknown user: {}'.format(email))
        return tk.redirect_to(tk.url_for('user.login'))
    user_extras = user[0].plugin_extras
    if user_extras and user_extras.get('idp', None) == 'google':
        log.info(u'User requested reset link for google user: {}'.format(email))
        h.flash_error('Invalid email address')
        return tk.redirect_to(tk.url_for('user.login'))
    return RequestResetView().post()

def force_sso_login():
    """Κλείνει εντελώς το standard login - επιτρέπει μόνο SSO,
    αν το internal login δεν είναι ενεργοποιημένο, δηλαδή
    ckanext.keycloak.enable_ckan_internal_login = false
    """

    # Αν το internal login είναι ενεργοποιημένο, χρησιμοποίησε την αρχική CKAN login function
    if helpers.enable_internal_login():
        return original_login()

    if not helpers.enable_internal_login() and tk.request.method == 'POST':
        # Αν κάποιος προσπαθήσει POST, επιστρέφουμε error ή redirect
        log.warning('Standard login attempt blocked - SSO is required. Redirecting to SSO.')
        return tk.redirect_to('/user/sso')

    return original_login()

def sso_logout():
    """Logout από CKAN + Keycloak (+ cascade στη ΓΓΠΣ μέσω Keycloak)"""
    id_token = session.pop('keycloak_id_token', None)
    log.info(f"SSO Logout - id_token present: {id_token is not None}")

    # Κάνουμε πρώτα το CKAN logout
    original_logout()

    # Αν έχουμε id_token, redirect στο Keycloak end_session_endpoint
    if id_token:
        post_logout_uri = tk.config.get('ckanext.keycloak.post_logout_redirect_uri',
                                        environ.get('CKANEXT__KEYCLOAK__POST_LOGOUT_REDIRECT_URI',
                                                     redirect_uri))
        base_url = server_url.rstrip('/')
        logout_url = (
            f"{base_url}/realms/{realm_name}/protocol/openid-connect/logout"
            f"?id_token_hint={id_token}"
            f"&post_logout_redirect_uri={post_logout_uri}"
        )
        log.info(f"SSO Logout - redirect to: {logout_url}")
        return tk.redirect_to(logout_url)

    return tk.redirect_to('/')


keycloak.add_url_rule('/sso', view_func=sso)
keycloak.add_url_rule('/sso_login', view_func=sso_login)
keycloak.add_url_rule('/login', view_func=force_sso_login, methods=['GET','POST'])
keycloak.add_url_rule('/_logout', view_func=sso_logout, methods=['GET', 'POST'])
# keycloak.add_url_rule('/reset_password', view_func=reset_password, methods=['POST'])

def get_blueprint():
    return keycloak