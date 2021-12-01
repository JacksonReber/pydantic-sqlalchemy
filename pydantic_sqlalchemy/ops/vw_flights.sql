SELECT
f."TRANSACTIONID",
f."FLIGHTDATE",
ff."DISTANCEGROUP",
ff."DEPDELAYGT15",
ff."NEXTDAYARR",
ol."AIRPORTNAME" AS "ORIGAIRPORTNAME",
dl."AIRPORTNAME" AS "DESTAIRPORTNAME",
f."DEPDELAY",
f."TAXIOUT",
f."TAXIIN",
f."ARRDELAY",
dl."AIRPORTCODE" AS "DESTAIRPORTCODE",
ol."AIRPORTCODE" AS "ORIGINAIRPORTCODE",
s."AIRLINENAME",
dl."STATE_ISO" AS "DESTSTATE",
ol."STATE_ISO" AS "ORIGINSTATE",
f."CRSARRTIME",
f."CRSDEPTIME"
FROM "FACT_FLIGHTS" ff
LEFT JOIN "DIM_SERVICE" s
USING (service_id)
LEFT JOIN "DIM_FLIGHT" f
USING (flight_id)
LEFT JOIN "DIM_LOCATION" ol
ON ff.origin_location_id = ol.location_id
LEFT JOIN "DIM_LOCATION" dl
ON ff.destination_location_id = dl.location_id