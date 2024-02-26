from pydantic import BaseModel

from typing import Any, Optional

from dynaconf import Dynaconf


class AuthSettings(BaseModel):
    enabled: bool = True
    interactive: bool = True
    authorization_url: Optional[str] = None
    token_url: Optional[str] = None
    client_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


class Settings(BaseModel):
    auth: AuthSettings | None
    stac_api_url: str | None
    collection_auth_info: dict[str, Any] | None


def _get_dynaconf():
    return Dynaconf(
        envvar_prefix="STACBLD_",
        env_switcher="STACBLD_ENV",
        settings_files=["default.yaml", "production.yaml", "development.yaml", ".secrets.yaml"],
        environments=True,
        default_env="default",
        env="development",
        merge_enabled=True,
    )


def get_stac_api_settings() -> Settings:
    _settings = _get_dynaconf()

    auth_out = AuthSettings()
    auth_in = _settings.get("auth")
    if auth_in:
        auth_out.enabled = auth_in.get("enabled", False)
        auth_out.interactive = auth_in.get("interactive", True)

        auth_out.authorization_url = auth_in.get("authorization_url")
        auth_out.token_url = auth_in.get("token_url")
        auth_out.client_id = auth_in.get("client_id")
        auth_out.username = auth_in.get("username")
        auth_out.password = auth_in.get("password")

    stac_api_url = _settings.get("stac", {}).get("api")
    collection_auth_info = _settings.get("collection_auth_info")

    return Settings(stac_api_url=stac_api_url, auth=auth_out, collection_auth_info=collection_auth_info)
