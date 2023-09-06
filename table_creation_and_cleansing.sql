/* The 'accident_yyyy' tables contain general information about each particular incident. 
They are stored in the bigquery-public-data library, so I will need to create new tables within my 
own BigQuery project for further analysis and transformation. 

This query selects all the desired columns and places them into a new table. Since this dataset stores each year's data in 
a separate table, I executed the query for each year by replacing '2015' in the FROM and CREATE TABLE statements with subsequent years. */

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2015 AS
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
  
  /* The 'work_zone' column displays the type of workers present, but I only wanted to know whether or not there were 
  any workers present at all, so I used a CASE statement to create a column showing this. */
  CASE
    WHEN work_zone = 'None'THEN 'False'
    WHEN work_zone = 'Work Zone, Type Unknown' OR work_zone IS NULL THEN 'Unknown'
    ELSE 'True'
  END AS is_work_zone,
  light_condition_name AS lighting_conditions,
  atmospheric_conditions_name AS weather,
  school_bus_related AS school_bus_involved
FROM
  bigquery-public-data.nhtsa_traffic_fatalities.accident_2015;
  
/* This query uses UNION clauses to combine all the tables created with the query above, making one table that 
contains incident data for all the years. */

CREATE TABLE us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all AS
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2015
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2016
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2017
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2018
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2019
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.accidents_2020;

/* This query creates new tables that contain data about what drivers were distracted by. Just like before, 
I executed the query for each year by replacing '2015' in the FROM and CREATE TABLE statements with subsequent years. */

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.distract_2015 AS
SELECT
  consecutive_number AS incident_id,
  vehicle_number,
  driver_distracted_by_name AS driver_distraction
FROM
  `bigquery-public-data.nhtsa_traffic_fatalities. distract_2015`;

/* This query uses UNION clauses to combine all the tables created with the query above, making one table that 
contains driver distraction data for all the years. */  

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.distract_all AS
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.distract_2015
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.distract_2016
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.distract_2017
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.distract_2018
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.distract_2019
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.distract_2020;

/* This query creates new tables that contain data about how drivers were impaired. Just like before, 
I executed the query for each year by replacing '2015' in the FROM and CREATE TABLE statements with subsequent years. */

CREATE TABLE 
  us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2015 AS
SELECT
  consecutive_number AS incident_id,
  vehicle_number,
  condition_impairment_at_time_of_crash_driver_name AS driver_impairment
FROM
  `bigquery-public-data.nhtsa_traffic_fatalities. drimpair_2015`;

/* This query uses UNION clauses to combine all the tables created with the query above, making one table that 
contains driver impairment data for all the years. */  
  
CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_all AS
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2015
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2016
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2017
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2018
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2019
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.drimpair_2020;

/* This query creates new tables that contain data about the actions of non-motorists (people involved that weren't inside a car) 
that may have contributed to the incident. Just like before, I executed the query for each year by replacing '2015' in the 
FROM and CREATE TABLE statements with subsequent years. */

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2015 AS
SELECT
  consecutive_number AS incident_id,
  vehicle_number,
  person_number,
  non_motorist_contributing_circumstances_name AS non_motorist_contributing_action
FROM
  `bigquery-public-data.nhtsa_traffic_fatalities. nmcrash_2015`;

/* This query uses UNION clauses to combine all the tables created with the query above, making one table that 
contains data about the actions of non-motorists for all the years. */  

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_all AS
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2015
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2016
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2017
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2018
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2019
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.nmactions_2020;

/* This query creates new tables that contain data about the obstacles drivers attempted to avoid. Just like before, 
I executed the query for each year by replacing '2015' in the FROM and CREATE TABLE statements with subsequent years. */

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2015 AS
SELECT
  consecutive_number AS incident_id,
  vehicle_number,
  driver_maneuvered_to_avoid_name AS obstacle_to_avoid
FROM
  `bigquery-public-data.nhtsa_traffic_fatalities. maneuver_2015`;

/* This query uses UNION clauses to combine all the tables created with the query above, making one table that 
contains data about the obstacles drivers attempted to avoid for all the years. */  

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_all AS
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2015
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2016
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2017
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2018
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2019
UNION ALL
SELECT * FROM us-traffic-incidents-analysis.nhtsa_data_tables.obstacles_2020;
