import datetime
import uuid
import pandas as pd
from numpy import genfromtxt
from sqlalchemy.dialects.postgresql import UUID, TIME, DATE, TIMESTAMP
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine, text, Boolean, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker, declarative_base
import datetime
from schemas import SuperModel, LocationModel, FlightModel, ServiceModel, FactFlightModel


def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda s: str(s)})
    return data.tolist()


Base = declarative_base()

DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'changeme'
DB_HOST = 'localhost'
DB_PORT = '5432'

SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@"
    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
)





engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

UUID_GENERATOR = text("uuid_generate_v4()")


class SuperStore(Base):
    __tablename__ = "superstore"

    vwf_id = Column(
        UUID(as_uuid=True),
        default=uuid.uuid4,
        primary_key=True,
        nullable=False,
    )
    TRANSACTIONID = Column(Integer, nullable=True)
    FLIGHTDATE = Column(DATE, nullable=True)
    CRSDEPTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    DEPTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    DEPDELAY = Column(Integer, nullable=True)
    TAXIOUT = Column(Integer, nullable=True)
    WHEELSOFF = Column(TIMESTAMP(timezone=True), nullable=True)
    WHEELSON = Column(TIMESTAMP(timezone=True), nullable=True)
    TAXIIN = Column(Integer, nullable=True)
    CRSARRTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    ARRTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    ARRDELAY = Column(Integer, nullable=True)
    CRSELAPSEDTIME = Column(Integer, nullable=True)
    ACTUALELAPSEDTIME = Column(Integer, nullable=True)
    DISTANCE = Column(Integer, nullable=True)
    DISTANCEGROUP = Column(String, nullable=True)
    DEPDELAYGT15 = Column(Boolean, nullable=True)
    NEXTDAYARR = Column(Boolean, nullable=True)
    AIRLINECODE = Column(String, nullable=True)
    AIRLINENAME = Column(String, nullable=True)
    TAILNUM = Column(String, nullable=True)
    FLIGHTNUM = Column(Integer, nullable=True)
    ORIGINAIRPORTCODE = Column(String, nullable=True)
    ORIGAIRPORTNAME = Column(String, nullable=True)
    ORIGINCITYNAME = Column(String, nullable=True)
    ORIGINSTATE = Column(String, nullable=True)
    ORIGINSTATENAME = Column(String, nullable=True)
    DESTAIRPORTCODE = Column(String, nullable=True)
    DESTAIRPORTNAME = Column(String, nullable=True)
    DESTCITYNAME = Column(String, nullable=True)
    DESTSTATE = Column(String, nullable=True)
    DESTSTATENAME = Column(String, nullable=True)


class FlightDim(Base):
    __tablename__ = "DIM_FLIGHT"

    flight_id = Column(
        UUID(as_uuid=True),
        default=uuid.uuid4,
        primary_key=True,
        nullable=False,
    )
    TRANSACTIONID = Column(Integer, nullable=True)
    FLIGHTDATE = Column(DATE, nullable=True)
    CRSDEPTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    DEPTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    DEPDELAY = Column(Integer, nullable=True)
    TAXIOUT = Column(Integer, nullable=True)
    WHEELSOFF = Column(TIMESTAMP(timezone=True), nullable=True)
    WHEELSON = Column(TIMESTAMP(timezone=True), nullable=True)
    TAXIIN = Column(Integer, nullable=True)
    CRSARRTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    ARRTIME = Column(TIMESTAMP(timezone=True), nullable=True)
    ARRDELAY = Column(Integer, nullable=True)
    CRSELAPSEDTIME = Column(Integer, nullable=True)
    ACTUALELAPSEDTIME = Column(Integer, nullable=True)
    DISTANCE = Column(Integer, nullable=True)


class ServiceDim(Base):
    __tablename__ = "DIM_SERVICE"

    service_id = Column(
        UUID(as_uuid=True),
        default=uuid.uuid4,
        primary_key=True,
        nullable=False,
    )
    AIRLINECODE = Column(String, nullable=True)
    AIRLINENAME = Column(String, nullable=True)
    TAILNUM = Column(String, nullable=True)
    FLIGHTNUM = Column(Integer, nullable=True)


class LocationDim(Base):
    __tablename__ = "DIM_LOCATION"

    location_id = Column(
        UUID(as_uuid=True),
        default=uuid.uuid4,
        primary_key=True,
        nullable=False,
    )
    AIRPORTCODE = Column(String, nullable=True)
    AIRPORTNAME = Column(String, nullable=True)
    CITYNAME = Column(String, nullable=True)
    STATE = Column(String, nullable=True)
    STATENAME = Column(String, nullable=True)


class FactFlight(Base):
    __tablename__ = "FACT_FLIGHTS"

    ff_id = Column(
        UUID(as_uuid=True),
        default=uuid.uuid4,
        primary_key=True,
        nullable=False,
    )
    DISTANCEGROUP = Column(String, nullable=True)
    DEPDELAYGT15 = Column(Boolean, nullable=True)
    NEXTDAYARR = Column(Boolean, nullable=True)

    origin_location_id = Column(UUID)  # ForeignKey("LOCATION_DIM.location_id")
    destination_location_id = Column(UUID)  # ForeignKey("LOCATION_DIM.location_id")
    service_id = Column(UUID)  # ForeignKey("SERVICE_DIM.service_id")
    flight_id = Column(UUID)

    created_at = Column(TIMESTAMP(timezone=True), default=datetime.datetime.utcnow)
    updated_at = Column(TIMESTAMP(timezone=True))


Base.metadata.create_all(engine)

LocalSession = sessionmaker(bind=engine)

db: Session = LocalSession()



with open("ops/init_location_dim.sql", "r") as sql_file:
    escaped_sql = text(sql_file.read())
    db.execute(escaped_sql)

with open("ops/init_service_dim.sql", "r") as sql_file:
    escaped_sql = text(sql_file.read())
    db.execute(escaped_sql)

with open("ops/init_flight_dim.sql", "r") as sql_file:
    escaped_sql = text(sql_file.read())
    db.execute(escaped_sql)

with open("ops/init_fact_flights.sql", "r") as sql_file:
    escaped_sql = text(sql_file.read())
    db.execute(escaped_sql)


def add_unique_service(
        seen_services,
        services,
        airline_code,
        airline_name,
        tailnum,
        flightnum
):
    if airline_name not in seen_services:
        new_uuid = str(uuid.uuid4())
        seen_services[airline_name] = new_uuid
        service_schema = ServiceModel(
            service_id=new_uuid,
            AIRLINECODE=airline_code,
            AIRLINENAME=airline_name,
            TAILNUM=tailnum,
            FLIGHTNUM=flightnum
        )
        services.append(ServiceDim(**service_schema.dict()))


def add_unique_location(
        seen_locations,
        locations,
        airport_code,
        airport_name,
        airport_city,
        airport_state,
        airport_state_name
):
    if airport_name not in seen_locations:
        new_uuid = str(uuid.uuid4())
        seen_locations[airport_name] = new_uuid
        location_schema = LocationModel(
            location_id=new_uuid,
            AIRPORTCODE=airport_code,
            AIRPORTNAME=airport_name,
            CITYNAME=airport_city,
            STATE=airport_state,
            STATENAME=airport_state_name
        )
        locations.append(LocationDim(**location_schema.dict()))


df = pd.read_csv('./ops/flights.csv')
df.to_sql(con=engine, index_label='vwf_id', name=SuperStore.__tablename__, if_exists='replace')


def get_all():
    all_data = db.query(SuperStore).all()
    flights = []
    fact_flights = []
    seen_flights = {}
    seen_locations = {}
    locations = []
    seen_services = {}
    services = []

    for data in all_data:
        data = SuperModel(**data.__dict__)

        add_unique_location(
            seen_locations=seen_locations,
            locations=locations,
            airport_code=data.ORIGINAIRPORTCODE,
            airport_name=data.ORIGAIRPORTNAME,
            airport_city=data.ORIGINCITYNAME,
            airport_state=data.ORIGINSTATE,
            airport_state_name=data.ORIGINSTATENAME
        )
        add_unique_location(
            seen_locations=seen_locations,
            locations=locations,
            airport_code=data.DESTAIRPORTCODE,
            airport_name=data.DESTAIRPORTNAME,
            airport_city=data.DESTCITYNAME,
            airport_state=data.DESTSTATE,
            airport_state_name=data.DESTSTATENAME
        )
        add_unique_service(
            seen_services=seen_services,
            services=services,
            airline_code=data.AIRLINECODE,
            airline_name=data.AIRLINENAME,
            tailnum=data.TAILNUM,
            flightnum=data.FLIGHTNUM

        )
        flight_uuid = str(uuid.uuid4())
        flight_schema = FlightModel(
            flight_id=flight_uuid,
            TRANSACTIONID=data.TRANSACTIONID,
            FLIGHTDATE=data.FLIGHTDATE,
            CRSDEPTIME=data.CRSDEPTIME,
            DEPTIME=data.DEPTIME,
            DEPDELAY=data.DEPDELAY,
            TAXIOUT=data.TAXIOUT,
            WHEELSOFF=data.WHEELSOFF,
            WHEELSON=data.WHEELSON,
            TAXIIN=data.TAXIIN,
            CRSARRTIME=data.CRSARRTIME,
            ARRTIME=data.ARRTIME,
            ARRDELAY=data.ARRDELAY,
            CRSELAPSEDTIME=data.CRSELAPSEDTIME,
            ACTUALELAPSEDTIME=data.ACTUALELAPSEDTIME,
            DISTANCE=data.DISTANCE
        )
        seen_flights[data.TRANSACTIONID] = flight_uuid
        flights.append(FlightDim(**flight_schema.dict()))

    for data in all_data:
        data = SuperModel(**data.__dict__)
        fact_flights_schema = FactFlightModel(
            DISTANCEGROUP=data.DISTANCEGROUP,
            DEPDELAYGT15=data.DEPDELAYGT15,
            NEXTDAYARR=data.NEXTDAYARR,
            origin_location_id=str(seen_locations[data.ORIGAIRPORTNAME]),
            destination_location_id=str(seen_locations[data.DESTAIRPORTNAME]),
            service_id=str(seen_services[data.AIRLINENAME]),
            flight_id=str(seen_flights[data.TRANSACTIONID])
        )
        fact_flights.append(FactFlight(**fact_flights_schema.dict()))

    db.add_all(locations)
    db.add_all(services)
    db.add_all(flights)

    try:

        db.commit()
    except SQLAlchemyError as e:
        print('1')
        import pdb;
        pdb.set_trace()

    try:
        db.add_all(fact_flights)
        db.commit()
    except SQLAlchemyError as e:
        print('2')
        import pdb;
        pdb.set_trace()

    try:
        db.execute(text('DROP TABLE IF EXISTS superstore;'))
        db.commit()
    except SQLAlchemyError as e:
        print('3')
        import pdb;
        pdb.set_trace()

    print('zuggma')


get_all()
