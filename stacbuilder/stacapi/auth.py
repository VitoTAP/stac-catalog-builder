import logging


import requests.auth
from requests_auth import (
    OAuth2AuthorizationCodePKCE,
    OAuth2ResourceOwnerPasswordCredentials,
)


from stacbuilder.stacapi.config import AuthSettings


logger = logging.getLogger(__name__)


def get_auth(auth_settings: AuthSettings) -> requests.auth.AuthBase | None:
    if auth_settings.enabled:
        if auth_settings.interactive and auth_settings.authorization_url:
            logger.info("Using interactive login via authorization code flow")
            return OAuth2AuthorizationCodePKCE(
                authorization_url=auth_settings.authorization_url,
                token_url=auth_settings.token_url,
                client_id=auth_settings.client_id,
            )
        elif auth_settings.username and auth_settings.password:
            logger.info(
                f"Using login with username {auth_settings.username} via resource owner password credentials flow",
            )
            return OAuth2ResourceOwnerPasswordCredentials(
                token_url=auth_settings.token_url,
                username=auth_settings.username,
                password=auth_settings.password,
                client_id=auth_settings.client_id,
            )
        else:
            raise Exception(
                "Auth not properly configured: either use interactive login or supply username and password."
            )
    else:
        return None
