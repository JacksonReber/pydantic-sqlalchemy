
CREATE TABLE IF NOT EXISTS "FACT_FLIGHTS" (
    ff_id uuid PRIMARY KEY,
    service_id uuid,
    origin_location_id uuid,
    destination_location_id uuid,
    flight_id uuid,
    "DISTANCEGROUP" varchar,
    "NEXTDAYARR" boolean,
    "DEPDELAYGT15" boolean,

    FOREIGN KEY (service_id) REFERENCES public."DIM_SERVICE" (service_id),
    FOREIGN KEY (origin_location_id) REFERENCES public."DIM_LOCATION" (location_id),
    FOREIGN KEY (destination_location_id) REFERENCES public."DIM_LOCATION" (location_id),
    FOREIGN KEY (flight_id) REFERENCES public."DIM_FLIGHT" (flight_id),
	created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP
);