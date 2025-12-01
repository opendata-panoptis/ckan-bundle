import logging
from keycloak import KeycloakOpenID, KeycloakAdmin

from ckanext.keycloak.helpers import force_keycloak_reauth

log = logging.getLogger(__name__)

class KeycloakClient:
    def __init__(self, server_url, client_id, realm_name, client_secret_key):
        self.server_url = server_url
        self.client_id = client_id
        self.realm_name = realm_name
        self.client_secret_key = client_secret_key
        
    def get_keycloak_client(self):
        return KeycloakOpenID(
            server_url=self.server_url, client_id=self.client_id, realm_name=self.realm_name, client_secret_key=self.client_secret_key
        )

    def get_auth_url(self, redirect_uri, max_age=None, prompt=None):
        """
        Generate authorization URL with optional OIDC parameters
        """
        # Δημιουργία βασικού URL
        base_auth_url = self.get_keycloak_client().auth_url(redirect_uri=redirect_uri, scope="openid profile email")

        if force_keycloak_reauth():

            # Προσθήκη επιπλέον παραμέτρων
            additional_params = []

            if max_age is not None:
                additional_params.append(f"max_age={max_age}")

            if prompt is not None:
                additional_params.append(f"prompt={prompt}")

            # Συνδυασμός του URL με τις επιπλέον παραμέτρους
            if additional_params:
                separator = "&" if "?" in base_auth_url else "?"
                return f"{base_auth_url}{separator}{'&'.join(additional_params)}"

        return base_auth_url

    def get_token(self, code, redirect_uri):
        return self.get_keycloak_client().token(grant_type="authorization_code", code=code, redirect_uri=redirect_uri)

    def get_user_info(self, token):
        return self.get_keycloak_client().userinfo(token.get('access_token'))

    def get_user_groups(self, token):
        return self.get_keycloak_client().userinfo(token).get('groups', [])

    def get_keycloak_admin(self):
        return KeycloakAdmin(
            username="admin",
        )