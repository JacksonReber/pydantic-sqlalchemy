
CREATE TABLE IF NOT EXISTS "DIM_SERVICE" (
    service_id uuid,
    created_at timestamp,
    updated_at timestamp,
    "AIRLINECODE" varchar,
    "AIRLINENAME" varchar,
    "TAILNUM" varchar,
    "FLIGHTNUM" int,
    PRIMARY KEY (service_id)
);
