
CREATE TABLE IF NOT EXISTS "DIM_LOCATION" (
    location_id uuid,
    created_at timestamp,
    updated_at timestamp,
    "AIRPORTCODE" varchar,
    "AIRPORTNAME" varchar,
    "CITYNAME" varchar,
    "STATE" varchar,
    "STATENAME" varchar,
    PRIMARY KEY (location_id)
);


