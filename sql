/* This query creates tables with all the desired columns for analysis. 
There are separate tables for each year, so I replaced '2016' with the relevant year and executed the query for each year */

CREATE OR REPLACE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2016 AS
SELECT 
  consecutive_number AS incident_id,
  state_name AS state,
  county_name AS county,
  city_name AS city,
  number_of_motor_vehicles_in_transport_mvit AS num_vehicles_involved,
  month_of_crash AS month,
  day_of_crash AS day_of_the_month,
  year_of_crash AS year,
  day_of_week,
  hour_of_crash AS hour,
  minute_of_crash AS minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  route_signing_name AS road_type,
  land_use_name AS population_density,
  latitude,
  longitude,
  special_jurisdiction_name AS special_jurisdiction,
  first_harmful_event_name AS first_harmful_event,
  manner_of_collision_name AS collision_manner,
  relation_to_junction_specific_location_name AS junction_type,
  /* The work_zone column displays the type of workers present, but I only wanted to know whether or not there were 
  any workers present at all, so I used a CASE statement to make a column showing this, */
  CASE
    WHEN work_zone = 'None'THEN 'False'
    WHEN work_zone = 'Work Zone, Type Unknown' OR work_zone IS NULL THEN 'Unknown'
    ELSE 'True'
  END AS is_work_zone,
  light_condition_name AS lighting_conditions,
  atmospheric_conditions_name AS weather,
  school_bus_related AS school_bus_involved
FROM
  bigquery-public-data.nhtsa_traffic_fatalities.accident_2016
  
