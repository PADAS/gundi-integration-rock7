from datetime import datetime

import pydantic

from app.actions.core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    InternalActionConfiguration,
    PullActionConfiguration,
)
from app.services.errors import ConfigurationNotFound
from app.services.utils import (
    FieldWithUIOptions,
    GlobalUISchemaOptions,
    UIOptions,
    find_config_for_action,
)

class ServiceCredentialsConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str = pydantic.Field(..., title="Username", description="Username for a Rock7 account.")
    password: pydantic.SecretStr = pydantic.Field(..., format="password", 
                                                  title="Password",
                                                  description="Password for a Rock7 account.")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "password"
        ],
    )                                                            


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return ServiceCredentialsConfig.parse_obj(auth_config.data)


class FetchLocationsConfig(PullActionConfiguration):
    subject_type: str = pydantic.Field("vehicle", title="Subject Type", description="Subject type for the observations.")