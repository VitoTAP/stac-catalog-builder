from requests import delete
import getpass
from openeo.rest.auth.oidc import (
    OidcClientInfo,
    OidcProviderInfo,
    OidcResourceOwnerPasswordAuthenticator,
    OidcAuthCodePkceAuthenticator,
)

class VitoStacApiAuthentication:
    """Class that handles authentication for the VITO STAC API. https://stac.openeo.vito.be/"""

    def __init__(self, **kwargs):
        self.username = kwargs.get("username")
        self.password = kwargs.get("password")

    def __call__(self, request):
        request.headers["Authorization"] = self.get_access_token()
        return request

    def get_access_token(self) -> str:
        """Get API bearer access token via password flow.

        Returns
        -------
        str
            A string containing the bearer access token.
        """
        provider_info = OidcProviderInfo(
            issuer="https://sso.terrascope.be/auth/realms/terrascope"
        )

        client_info = OidcClientInfo(
            client_id="terracatalogueclient",
            provider=provider_info,
        )

        if self.username and self.password:
            authenticator = OidcResourceOwnerPasswordAuthenticator(
                client_info=client_info, username=self.username, password=self.password
            )
        else:
            authenticator = OidcAuthCodePkceAuthenticator(
                client_info=client_info
            )

        tokens = authenticator.get_tokens()

        return f"Bearer {tokens.access_token}"

tokenProvider = VitoStacApiAuthentication(username="jeroenwannijn", password=getpass.getpass())
url = "https://stac.openeo.vito.be/collections/PLANETSCOPE_V01"
headers = {
    'Authorization': tokenProvider.get_access_token(),
    'Content-Type': 'application/json'
}

# Delete collection 
response = delete(url, headers=headers)
if response.status_code == 403:
    print("Access denied: Invalid collection authorizations")
elif response.status_code == 200:
    print("Success:", response.json())
else:
    print("Error:", response.status_code, response.text)