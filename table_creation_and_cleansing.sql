/* The 'accident_yyyy' tables in the dataset contain general information about each particular incident. Because they are stored in 
a public data library, I needed to create new tables within my own BigQuery project for further cleansing and analysis. 

The query below selects all the columns I chose to analyze from the public dataset tables and places them into a new table called 
'accidents_all' within my own BigQuery project. This public dataset stores each year's data in a separate table, so I needed to 
combine data from the years 2017 through 2020 into a single table (accidents_all). Check the FROM and WHERE statements to see how 
I accomplished this. */

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all AS
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

  /* After looking at the distinct values for 'work_zone_name', I learned it displays the type of workers present, but I only 
  wanted to know whether or not there were any workers present at all, so I used a CASE statement to create a column showing this. */
  
  CASE
    WHEN work_zone_name = 'None' THEN 'False'
    WHEN work_zone_name IS NULL THEN 'Unknown'
    ELSE 'True'
  END AS is_work_zone,
  light_condition_name AS lighting_conditions,

  /* The 'atmospheric_conditions_name' column is changed to 'atmospheric_conditions_1_name' in the 'accident_2020' table, so 
  I used a CASE statement to reference the correct column name when selecting data from the 2020 table. */
  
  CASE
    WHEN _TABLE_SUFFIX = '2020' THEN atmospheric_conditions_1_name
    ELSE atmospheric_conditions_name
  END AS weather,

  /* To combine the 2017 through 2020 tables as mentioned earlier, I used the '*' wildcard character to select all the tables 
  beginning with ' accident_' (which includes the accident tables for all years in the public dataset). Then, I used _TABLE_SUFFIX 
  in the WHERE clause to ensure only wildcard values between 2017 and 2020 were included. I converted the wildcard values to 
  integers with the CAST function to ensure the BETWEEN operator compared the years accurately. */
  
FROM
  `bigquery-public-data.nhtsa_traffic_fatalities. accident_*`
WHERE
  CAST(_TABLE_SUFFIX AS INT64) BETWEEN 2017 AND 2020;

/* I also decided to JOIN a table that contains the population of each state so that I could calculate per capita 
figures when comparing metrics between states. I used Excel to retrieve a table with 2020 state population data from 
the web, then I saved it as a csv file and uploaded it to my BigQuery project. I joined the data from the state 
population table to the 'accidents_all' table with the query below. */

CREATE OR REPLACE TABLE 
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all AS
SELECT
  a.*,
  p.pop_2020 AS state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all AS a
LEFT JOIN
  us-traffic-incidents-analysis.nhtsa_data_tables.states_pop AS p
ON
  a.state = p.state;

/* I created a new duplicate table of 'accidents_all' so that I could clean and validate the data without
modifying the original table. */

CREATE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  *
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all;

/* To get an overview of how the data was formatted, I queried all the columns and the first 100 rows. */

SELECT
  *
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
LIMIT
  100;

/* I noticed that many of the entries for 'county' end with an identifier number in parentheses, such as 'HARRIS (201)'. 
To remove these, I used the first STRPOS function to detect columns that end with an identifier, then the SUBSTR function with 
a nested STRPOS function to extract just the county name. Finally, I used a CASE statment to make sure that only the entries 
with the identifier were modified. I used the CREATE OR REPLACE TABLE statment to replace just the 'county' column while keeping 
all the other columns the same. */

CREATE OR REPLACE TABLE 
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  CASE
    WHEN STRPOS(county, ' (') > 0
      THEN SUBSTR(county, 1, STRPOS(county, ' (') - 1)
    ELSE county
  END AS county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  population_density,
  latitude,
  longitude,
  special_jurisdiction,
  first_harmful_event,
  collision_manner,
  junction_type,
  is_work_zone,
  lighting_conditions,
  weather,
  state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* After running the query below, I saw there were no more entries with the identifier in parentheses at the end. */

SELECT
  county
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
WHERE
  county LIKE '%(%';

/* While checking for duplicates, I ran the query below to view the distinct values for 'collision_manner' and discovered 
some duplicate categories. */

SELECT
  collision_manner,
  COUNT(*) AS count_occurrences
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
GROUP BY
  collision_manner

/* To remove duplicates, I used a CASE statement to replace the duplicate categories for the 'collision_manner' with the 
corresponding categories that I wanted to keep. I used the CREATE OR REPLACE TABLE statment to replace just the 
'collision_manner' column while keeping all the others the same. */

CREATE OR REPLACE TABLE 
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  population_density,
  latitude,
  longitude,
  special_jurisdiction,
  first_harmful_event,
  CASE
    WHEN collision_manner = 'The First Harmful Event was Not a Collision with a Motor Vehicle in Transport' 
      THEN 'Not a Collision with Motor Vehicle In-Transport'
    WHEN collision_manner IN('Reported as Unknown','Not Reported') 
      THEN 'Unknown'
    ELSE collision_manner
  END AS collision_manner,
  junction_type,
  is_work_zone,
  lighting_conditions,
  weather,
  state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* When looking at the distinct values for the 'population_density' column, there were a few duplicates, so I 
decided to rewrite the table and consolidate them with a CASE statement as shown below. */

CREATE OR REPLACE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  CASE
    WHEN population_density IN('Trafficway Not in State Inventory','Not Reported') THEN 'Unknown'
    ELSE population_density
  END AS population_density,
  latitude,
  longitude,
  special_jurisdiction,
  first_harmful_event,
  collision_manner,
  junction_type,
  is_work_zone,
  lighting_conditions,
  weather,
  state_population
FROM
    us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* When looking at the distinct values for the 'first_harmful_event' column, there were many very specific categories 
and a few duplicates, so I decided to rewrite the table and consolidate them with a CASE statement as shown below. */

CREATE OR REPLACE TABLE 
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  population_density,
  latitude,
  longitude,
  special_jurisdiction,
  CASE
    WHEN first_harmful_event IN (
      'Motor Vehicle In-Transport',
      'Motor Vehicle in Motion Outside the Trafficway',
      'Railway Vehicle',
      'Jackknife (harmful to this vehicle)',
      'Road Vehicle on Rails',
      'Working Motor Vehicle'
    ) THEN 'Other Moving Vehicle'

    WHEN first_harmful_event IN (
      'Tree (Standing Only)',
      'Shrubbery',
      'Fence',
      'Wall',
      'Curb',
      'Building',
      'Mail Box',
      'Utility Pole/Light Support',
      'Guardrail Face',
      'Guardrail End',
      'Impact Attenuator/Crash Cushion',
      'Concrete Traffic Barrier',
      'Cable Barrier',
      'Traffic Sign Support',
      'Culvert',
      'Bridge Pier or Support',
      'Post, Pole or Other Supports',
      'Bridge Rail (Includes parapet)',
      'Traffic Signal Support',
      'Fire Hydrant',
      'Immersion or Partial Immersion',
      'Other Traffic Barrier',
      'Ground',
      'Snow Bank',
      'Bridge Overhead Structure',
      'Embankment',
      'Ditch',
      'Parked Motor Vehicle',
      'Other Fixed Object',
      'Unknown Fixed Object'
    ) THEN 'Fixed Objects Present'
    
    WHEN first_harmful_event IN (
      'Unknown Object Not Fixed',
      'Unknown',
      'Reported as Unknown',
      'Harmful Event, Details Not Reported',
      'Other Non-Collision'
    ) THEN 'Unknown or Unreported Events'
    
    WHEN first_harmful_event IN (
      'Non-Motorist on Personal Conveyance',
      'Pedestrian',
      'Pedalcyclist',
      'Live Animal',
      'Ridden Animal or Animal Drawn Conveyance'
    ) THEN 'Pedestrian/Non-Motorist Present'
    
    WHEN first_harmful_event IN (
      'Cargo/Equipment Loss or Shift (harmful to this vehicle)',
      'Cargo/Equipment Loss, Shift, or Damage [harmful]',
      'Other Object (not fixed)',
      'Thrown or Falling Object',
      'Object That Had Fallen From Motor Vehicle In-Transport',
      'Motor Vehicle In-Transport Strikes or is Struck by Cargo, Persons or Objects Set-in-Motion from/by 
Another Motor Vehicle In Transport',
      'Boulder'
    ) THEN 'Unsecured Cargo/Moving Objects'

    WHEN first_harmful_event IN (
      'Injured In Vehicle (Non-Collision)',
      'Fell/Jumped from Vehicle'
    ) THEN 'Person Injured In/Ejected From Vehicle'
    
    ELSE first_harmful_event
  END AS first_harmful_event,
  collision_manner,
  junction_type,
  is_work_zone,
  lighting_conditions,
  weather,
  state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* When looking at the distinct values for the 'junction_type' column, there were a few duplicates, so I decided 
to rewrite the table and consolidate them with a CASE statement as shown below. */

CREATE OR REPLACE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  population_density,
  latitude,
  longitude,
  special_jurisdiction,
  first_harmful_event,
  collision_manner,
  CASE
    WHEN junction_type IN('Not Reported','Reported as Unknown') THEN 'Unknown'
    WHEN junction_type = 'Driveway Access Related' THEN 'Driveway Access'
    WHEN junction_type = 'Entrance/Exit Ramp Related' THEN 'Entrance/Exit Ramp'
    WHEN junction_type = 'Intersection-Related' THEN 'Intersection'
    ELSE junction_type
  END AS junction_type,
  is_work_zone,
  lighting_conditions,
  weather,
  state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* When looking at the distinct values for the 'lighting_conditions' column, there were a few duplicates, so I decided 
to rewrite the table and consolidate them with a CASE statement as shown below. */

CREATE OR REPLACE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  population_density,
  latitude,
  longitude,
  special_jurisdiction,
  first_harmful_event,
  collision_manner,
  junction_type, 
  is_work_zone,
  CASE
    WHEN lighting_conditions IN('Reported as Unknown','Not Reported','Unknown') THEN 'Other'
    ELSE lighting_conditions
  END AS lighting_conditions,
  weather,
  state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* When looking at the distinct values for the 'weather' column, there were a few duplicates, so I decided 
to rewrite the table and consolidate them with a CASE statement as shown below. */

CREATE OR REPLACE TABLE
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2 AS
SELECT
  incident_id,
  state,
  county,
  city,
  num_vehicles_involved,
  month,
  day_of_the_month,
  year,
  day_of_week,
  hour,
  minute,
  timestamp_of_crash,
  number_of_fatalities,
  number_of_drunk_drivers,
  road_type,
  population_density,
  latitude,
  longitude,
  special_jurisdiction,
  first_harmful_event,
  collision_manner,
  junction_type, 
  is_work_zone,
  lighting_conditions,
  CASE
    WHEN weather IN('Not Reported','Reported as Unknown') THEN 'Unknown'
    WHEN weather = 'Blowing Snow' THEN 'Snow'
    ELSE weather
  END AS weather,
  state_population
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* The query below checks for any null values in each of the numerical columns in the 'accidents_all_v2' table. 
After running it, no columns were returned, which means there are no null values. */

SELECT
  *
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
WHERE
  hour IS NULL OR
  minute IS NULL OR
  month IS NULL OR
  year IS NULL OR 
  day_of_the_month IS NULL OR 
  day_of_week IS NULL OR 
  num_vehicles_involved IS NULL OR 
  number_of_fatalities IS NULL OR 
  number_of_drunk_drivers IS NULL OR
  state_population IS NULL;

/* The query below checks for any invalid values in each of the numerical columns in the 'accidents_all_v2' table. 
After running it, no columns were returned, which means there are no errors. */

SELECT
  *
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
WHERE
  (hour NOT BETWEEN 0 AND 23) OR
  (minute NOT BETWEEN 0 AND 59) OR
  (month NOT BETWEEN 1 AND 12) OR
  (year NOT BETWEEN 2017 AND 2020) OR 
  (day_of_the_month NOT BETWEEN 1 AND 31) OR 
  (day_of_week NOT BETWEEN 1 AND 7) OR 
  (num_vehicles_involved < 1 ) OR 
  (number_of_fatalities < 1) OR 
  (number_of_drunk_drivers < 0) OR
  (state_population NOT BETWEEN 570000 AND 39540000);

/* The query below checks for any invalid 'day_of_the_month' values depending on the month. It also accounts for 
2020 being a leap year. After running it, no columns were returned, which means there are no errors. */

SELECT
  *
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
WHERE
  (day_of_the_month > 30 AND month IN(4,6,9,11)) OR
  (day_of_the_month > 28 AND month = 2 AND year <> 2020) OR 
  (day_of_the_month > 29 AND month = 2 AND year = 2020);

/* The query below uses the MIN and MAX functions to return the earliest and latest date in the 'timestamp_of_crash' column to 
ensure all timestamps are between 2017 and 2020. */

SELECT
  MIN(timestamp_of_crash) AS earliest_date,
  MAX(timestamp_of_crash) AS latest_date
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2;

/* The query below checks for any null values in the remaining columns that I didn't check earlier. After running it, 
no columns were returned, which means there are no null values. */

SELECT
  *
FROM
  us-traffic-incidents-analysis.nhtsa_data_tables.accidents_all_v2
WHERE
  state IS NULL OR
  city IS NULL OR
  county IS NULL OR
  latitude IS NULL OR
  longitude IS NULL OR 
  timestamp_of_crash IS NULL;

/* At this point, all the data is clean and ready for analysis. I will connect to my BigQuery account within Tableau and extract 
my table to analyze the data. 

The links to my Google Slides presentation and Tableau visualizations are in the README file in this repository. */ 

