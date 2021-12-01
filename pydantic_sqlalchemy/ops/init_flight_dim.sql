
CREATE TABLE IF NOT EXISTS "DIM_FLIGHT" (

    flight_id uuid PRIMARY KEY,
    "TRANSACTIONID" int UNIQUE,
    "FLIGHTDATE" date,
    "CRSDEPTIME" timestamptz,
    "DEPTIME" timestamptz,
    "DEPDELAY" int,
    "TAXIOUT" int,
    "WHEELSOFF" timestamptz,
    "WHEELSON" timestamptz,
    "TAXIIN" int,
    "CRSARRTIME" timestamptz,
    "ARRTIME" timestamptz,
    "ARRDELAY" int,
    "CRSELAPSEDTIME" int,
    "ACTUALELAPSEDTIME" int,

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP
    );
