from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class AuthSettings:
    enabled: bool = True
    interactive: bool = True
    authorization_url: Optional[str] = None
    token_url: Optional[str] = None
    client_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class Settings:
    auth: AuthSettings
    stac_api_url: str
    collection_auth_info: dict[str, Any]
