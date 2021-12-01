from datetime import datetime, timedelta, time
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import pytz
from pydantic import BaseModel, types, validator
from pydantic.schema import date
from utils import time_zones


class SuperModel(BaseModel):
    ORIGINAIRPORTCODE: str
    DESTAIRPORTCODE: str
    FLIGHTDATE: str
    ACTUALELAPSEDTIME: int
    AIRLINECODE: str
    AIRLINENAME: str
    ARRDELAY: int
    ARRTIME: str
    CRSARRTIME: str
    CRSDEPTIME: str
    CRSELAPSEDTIME: int
    DEPDELAY: int
    DEPDELAYGT15: int
    DEPTIME: str
    DESTAIRPORTNAME: str
    DESTCITYNAME: str
    DESTSTATE: str
    DESTSTATENAME: str
    DISTANCE: str
    DISTANCEGROUP: str
    FLIGHTNUM: int
    NEXTDAYARR: int
    ORIGAIRPORTNAME: str
    ORIGINCITYNAME: str
    ORIGINSTATE: str
    ORIGINSTATENAME: str
    TAILNUM: str
    TAXIIN: int
    TAXIOUT: int
    TRANSACTIONID: int
    WHEELSOFF: str
    WHEELSON: str
    vwf_id: int

    @validator("ACTUALELAPSEDTIME", "CRSELAPSEDTIME", "TAXIIN", "TAXIOUT", "DEPDELAY", "ARRDELAY")
    def validate_to_minutes(cls, v):
        return int(v)

    @validator("CRSDEPTIME", "DEPTIME", "WHEELSOFF")
    def validate_origin_time(cls, v, values, **kwargs):
        t = str(v)
        if len(t) < 4:
            t = ((4 - len(t)) * '0') + t
        dte = str(values["FLIGHTDATE"])
        tz = pytz.timezone(time_zones[values["ORIGINAIRPORTCODE"]])
        yyyy = int(dte[:4])
        month = int(dte[5:7])
        day = int(dte[8:10])
        hour = int(t[:2])
        minute = int(t[-2:])
        dte = datetime(yyyy, month, day, hour, minute)
        dte = dte.replace(tzinfo=tz)
        return dte

    @validator("WHEELSON", "CRSARRTIME", "ARRTIME")
    def validate_dest_time(cls, v, values, **kwargs):
        t = str(v)
        if len(t) < 4:
            t = ((4 - len(t)) * '0') + t
        dte = str(values["FLIGHTDATE"])
        tz = pytz.timezone(time_zones[values["DESTAIRPORTCODE"]])
        yyyy = int(dte[:4])
        month = int(dte[5:7])
        day = int(dte[8:10])
        hour = int(t[:2])
        minute = int(t[-2:])
        dte = datetime(yyyy, month, day, hour, minute)
        dte = dte.replace(tzinfo=tz)
        return dte

    @validator("DEPDELAYGT15", "NEXTDAYARR")
    def validate_to_bool(cls, v):
        return bool(int(v))

    @validator("DISTANCE")
    def validate_to_distance(cls, v):
        return int(v[:-6])

    @validator("FLIGHTDATE")
    def validate_to_datetime(cls, v):
        return datetime.strptime(v, '%Y-%m-%d')


class FactFlightModel(BaseModel):
    ff_id: Optional[UUID]
    DISTANCEGROUP: str
    DEPDELAYGT15: bool
    NEXTDAYARR: bool
    origin_location_id: Optional[UUID]
    destination_location_id: Optional[UUID]
    service_id: Optional[UUID]
    flight_id: Optional[UUID]

    @validator("origin_location_id", "destination_location_id", "service_id", "flight_id")
    def validate_id(cls, v):
        return str(v)


class FlightModel(BaseModel):
    flight_id: Optional[UUID]
    TRANSACTIONID: int
    FLIGHTDATE: date
    CRSDEPTIME: datetime
    DEPTIME: datetime
    DEPDELAY: int
    TAXIOUT: int
    WHEELSOFF: datetime
    WHEELSON: datetime
    TAXIIN: int
    CRSARRTIME: datetime
    ARRTIME: datetime
    ARRDELAY: int
    CRSELAPSEDTIME: int
    ACTUALELAPSEDTIME: int
    DISTANCE: int


class ServiceModel(BaseModel):
    service_id: UUID
    AIRLINECODE: str
    AIRLINENAME: str
    TAILNUM: str
    FLIGHTNUM: int

    class Config:
        orm_mode = True


class LocationModel(BaseModel):
    location_id: UUID
    AIRPORTCODE: str
    AIRPORTNAME: str
    CITYNAME: str
    STATE: str
    STATENAME: str

    class Config:
        orm_mode = True
