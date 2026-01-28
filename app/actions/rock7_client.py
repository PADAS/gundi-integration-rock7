import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, AsyncGenerator, List
import httpx
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)

# API URLs
BASE_URL = "https://core.rock7.com"
ME_URL = BASE_URL + "/APIX/Contact/Me"
LOGIN_URL = BASE_URL + "/Operations"
TRACK_URL = BASE_URL + "/APIX/Position/RangeDeviceGroupMember"
OBJECT_URL = BASE_URL + "/T"

# Configuration
CHUNK_SIZE_DAYS = 30


class Group(BaseModel):
    """Represents a device group from the Rock7 API."""
    id: str = Field(alias="i")      # group ID used in API calls
    name: str = Field(alias="n")    # group name

    class Config:
        allow_population_by_field_name = True


class Device(BaseModel):
    """Represents a device from the Rock7 API."""
    id: str = Field(alias="i")              # internal ID used in API calls (deviceGroupMember)
    serial: Optional[str] = Field(alias="s", default=None)  # device serial number
    name: str = Field(alias="n")            # device name

    class Config:
        allow_population_by_field_name = True


class Location(BaseModel):
    """Location coordinates for a Gundi observation."""
    lat: float
    lon: float


class Observation(BaseModel):
    """
    A Gundi observation - matches the format expected by Gundi's API.
    
    Can be sent directly via send_observations_to_gundi() using .dict().
    """
    source: str                                    # device identifier
    source_name: str                                # device name
    type: str = "tracking-device"                  # observation type
    recorded_at: datetime                          # timestamp
    location: Location                             # lat/lon coordinates
    subject_type: Optional[str] = None             # optional subject type
    additional: Optional[Dict[str, Any]] = None    # extra data

    @classmethod
    def from_position(cls, position: Dict[str, Any], device: Device) -> "Observation":
        """Create an Observation from a raw Rock7 position dictionary."""
        lat = round(position.get('lat'), 6) if position.get('lat') is not None else None
        lng = round(position.get('lng'), 6) if position.get('lng') is not None else None
        timestamp_ms = position.get('at')
        
        recorded_at = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc) if timestamp_ms else None
        
        # Collect additional fields (excluding the ones we've already extracted)
        known_keys = {'lat', 'lng', 'at'}
        additional = {k: v for k, v in position.items() if k not in known_keys}
        
        return cls(
            source=device.serial,
            source_name=device.name,
            subject_type='vehicle',
            recorded_at=recorded_at,
            location=Location(lat=lat, lon=lng),
            additional=additional if additional else None
        )

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


@asynccontextmanager
async def rock7_session(username: str, password: str) -> AsyncGenerator[httpx.AsyncClient, None]:
    """
    Create an authenticated Rock7 API session.
    
    Usage:
        async with rock7_session(username, password) as session:
            account = await get_account_info(session)
            groups = await get_groups(session, account['accountId'])
            for group in groups:
                devices = await get_devices(session, group.id)
                for device in devices:
                    async for track_point in get_device_data(session, device, from_date, to_date):
                        # process track_point
    """
    async with httpx.AsyncClient() as session:
        login_request = {
            "sticky": "false",
            "u": username,
            "p": password,
            "Submit": "Log in"
        }
        login_result = await session.post(LOGIN_URL, data=login_request)
        login_result.raise_for_status()
        
        if not httpx.codes.is_success(login_result.status_code):
            raise Exception("Failed to authenticate with Rock7")
        
        logger.info("Authenticated with Rock7")
        yield session


async def get_account_info(session: httpx.AsyncClient) -> Dict[str, Any]:
    """Get account information for the authenticated user."""
    response = await session.post(ME_URL)
    response.raise_for_status()
    return response.json()['contact']


async def get_groups(session: httpx.AsyncClient, account_id: str) -> List[Group]:
    """Get all device groups for an account."""
    request_data = {"t": "a", "i": account_id}
    response = await session.post(OBJECT_URL, data=request_data)
    response.raise_for_status()
    return [Group.parse_obj(g) for g in response.json()['g']]


async def get_devices(session: httpx.AsyncClient, group_id: str) -> List[Device]:
    """Get all devices in a group."""
    request_data = {"t": "g", "i": group_id}
    response = await session.post(OBJECT_URL, data=request_data)
    response.raise_for_status()
    return [Device.parse_obj(d) for d in response.json()['m']]


async def get_device_data(
    session: httpx.AsyncClient,
    device: Device,
    from_date: datetime,
    to_date: datetime
) -> AsyncGenerator[Observation, None]:
    """
    Fetch track data for a device within a date range.
    
    Yields Observation objects for each position in the response.
    Data is fetched in chunks to handle large date ranges.
    
    Args:
        session: Authenticated httpx session
        device: The Device object (uses device.id for API, device.serial for source)
        from_date: Start of the date range
        to_date: End of the date range
        
    Yields:
        Observation objects ready to send to Gundi
    """
    lower_date = from_date
    while lower_date < to_date:
        upper_date = min(to_date, lower_date + timedelta(days=CHUNK_SIZE_DAYS))

        track_request = {
            "deviceGroupMember": device.id,
            "dateFrom": lower_date.strftime('%Y-%m-%d %H:%M:%S'),
            "dateTo": upper_date.strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(
            f"Loading track data for device {device.name} ({device.serial}) "
            f"from {track_request['dateFrom']} to {track_request['dateTo']}"
        )
        response = await session.post(TRACK_URL, data=track_request)
        response.raise_for_status()

        track_data = response.json()
        for member in track_data['members']:
            for position in member['positions']:
                if position.get('lat') is not None and position.get('lng') is not None:
                    yield Observation.from_position(position, device=device)
                else:
                    logger.warning(f"Skipping invalid position (missing coordinates): {position}")

        lower_date = upper_date

