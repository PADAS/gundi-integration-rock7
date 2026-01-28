from datetime import datetime, timezone, timedelta
from typing import AsyncGenerator, List
import json
import logging
from app.actions.configurations import ServiceCredentialsConfig, get_auth_config, FetchLocationsConfig
from app.actions.rock7_client import rock7_session, get_account_info, get_groups, get_devices, get_device_data, Observation
from app.services.activity_logger import activity_logger
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from gundi_core.schemas.v2 import Integration

logger = logging.getLogger(__name__)

state_manager = IntegrationStateManager()

# Batch size for sending observations to Gundi
OBSERVATION_BATCH_SIZE = 100


async def action_auth(integration, action_config: ServiceCredentialsConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    try:
        async with rock7_session(action_config.username, action_config.password.get_secret_value()) as session:
            account_info = await get_account_info(session)

    except Exception as e:
        return {"valid_credentials": False, "message": f"Failed to authenticate with Rock7: {e}"}

    return {"valid_credentials": account_info is not None, "account_info": account_info}


@activity_logger()
@crontab_schedule("*/10 * * * *")
async def action_fetch_locations(integration: Integration, action_config: FetchLocationsConfig):
    logger.info(f"Executing 'fetch_locations' action with integration {integration} and action_config {action_config}")

    auth_config = get_auth_config(integration)

    observations_batch: List[dict] = []
    total_sent = 0

    async for observation in fetch_locations(integration, auth_config):
        # Use json.loads(observation.json()) for JSON-serializable dict (Pydantic v1)
        observations_batch.append(json.loads(observation.json()))

        if len(observations_batch) >= OBSERVATION_BATCH_SIZE:
            await send_observations_to_gundi(observations=observations_batch, integration_id=integration.id)
            total_sent += len(observations_batch)
            logger.info(f"Sent batch of {len(observations_batch)} observations (total: {total_sent})")
            observations_batch = []

    # Send any remaining observations
    if observations_batch:
        await send_observations_to_gundi(observations=observations_batch, integration_id=integration.id)
        total_sent += len(observations_batch)
        logger.info(f"Sent final batch of {len(observations_batch)} observations (total: {total_sent})")

    return {"observations_sent": total_sent}


async def fetch_locations(
    integration: Integration,
    auth_config: ServiceCredentialsConfig
) -> AsyncGenerator[Observation, None]:
    """
    Generator that yields Observation objects for all devices
    in all groups, using incremental sync based on the last
    highest timestamp per device.
    """
    async with rock7_session(auth_config.username, auth_config.password.get_secret_value()) as session:
        account_info = await get_account_info(session)
        groups = await get_groups(session, account_info['accountId'])

        for group in groups:
            devices = await get_devices(session, group.id)

            for device in devices:
                # Fetch last known (highest) timestamp from state, or default to 21 days ago
                state = await state_manager.get_state(integration.id, "fetch_locations", device.id)  # type: ignore
                if state and state.get('last_high_timestamp'):
                    last_high_timestamp = datetime.fromisoformat(state['last_high_timestamp'])
                else:
                    last_high_timestamp = datetime.now(timezone.utc) - timedelta(days=21)

                max_timestamp_found = last_high_timestamp

                async for observation in get_device_data(
                    session, device, last_high_timestamp, datetime.now(timezone.utc)
                ):
                    yield observation

                    # Track the max timestamp for next sync
                    if observation.recorded_at > max_timestamp_found:
                        max_timestamp_found = observation.recorded_at

                # Update state with the new high-water timestamp for this device
                await state_manager.set_state(
                    integration.id,
                    "fetch_locations",
                    {"last_high_timestamp": max_timestamp_found.isoformat()},
                    device.id
                )
