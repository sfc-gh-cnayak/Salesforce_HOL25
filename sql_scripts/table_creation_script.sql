USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
CREATE OR REPLACE DATABASE salesforce_demo_db;
USE SCHEMA salesforce_demo_db.public;

CREATE OR REPLACE TABLE resort_guest_checkin AS
WITH names AS (
  SELECT ARRAY_CONSTRUCT('Alice', 'Bob', 'Carol', 'Dan', 'Eva', 'Frank', 'Grace', 'Hugo', 'Ivy', 'Jake') AS first_names,
         ARRAY_CONSTRUCT('Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis') AS last_names
)
SELECT
  'GUEST_' || LPAD(SEQ4()::STRING, 5, '0') AS guest_id,
  first_names[UNIFORM(0, 9, RANDOM())] || ' ' || last_names[UNIFORM(0, 6, RANDOM())] AS guest_name,
  'USA' AS country,
  UNIFORM(100, 900, RANDOM()) AS room_number,
  CURRENT_DATE - (90 - SEQ4()) AS checkin_datetime,
  CURRENT_DATE - (90 - SEQ4()) + 2 AS checkout_datetime,  -- exactly 2-day stays
  CASE WHEN UNIFORM(1, 10, RANDOM()) <= 2 THEN 'VIP' ELSE 'Regular' END AS vip_status,
  CASE UNIFORM(1, 3, RANDOM())
    WHEN 1 THEN 'family'
    WHEN 2 THEN 'solo'
    ELSE 'couple'
  END AS guest_type,
  CASE UNIFORM(1, 3, RANDOM())
    WHEN 1 THEN 'Gold'
    WHEN 2 THEN 'Silver'
    ELSE 'Bronze'
  END AS loyalty_tier,
  CASE UNIFORM(1, 3, RANDOM())
    WHEN 1 THEN 'website'
    WHEN 2 THEN 'travel_agent'
    ELSE 'mobile_app'
  END AS booking_source
FROM names,
     TABLE(GENERATOR(ROWCOUNT => 10000));  -- 🧹 Clean set of 10,000 guests




select * from resort_guest_checkin limit 10 ;


CREATE OR REPLACE TABLE resort_vision_inference AS
WITH object_data AS (
    SELECT 
        ARRAY_CONSTRUCT('passport','purse','jewelry','backpack','watch','phone','wallet','laptop') AS object_types,
        ARRAY_CONSTRUCT('Spa Lobby','Main Pool','Reception','Casino Floor','Villa Hallway','Kids Club','Lounge Bar','Gym') AS locations,
        ARRAY_CONSTRUCT('CAM001','CAM002','CAM003','CAM004','CAM005','CAM006','CAM007','CAM008') AS cameras
)
SELECT 
    locations[UNIFORM(0, 7, RANDOM())]::VARCHAR AS location_name,

    -- Inference timestamp in the last 90 days
    DATEADD(MINUTE, -UNIFORM(0, 60 * 24 * 90, RANDOM()), CURRENT_TIMESTAMP()) AS inference_timestamp,

    cameras[UNIFORM(0, 7, RANDOM())]::VARCHAR AS camera_id,

    object_types[UNIFORM(0, 7, RANDOM())]::VARCHAR AS object_type,

    'frame_' || LPAD(TO_VARCHAR(UNIFORM(1000, 9999, RANDOM())), 5, '0') || '.jpg' AS file_name,

    UNIFORM(1, 100, RANDOM()) <= 40 AS is_valuable,  -- ~40% marked valuable

    ROUND(UNIFORM(70, 100, RANDOM()) / 100.0, 2) AS label_confidence_score,

    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'personal_item'
        WHEN 2 THEN 'valuable'
        WHEN 3 THEN 'document'
        ELSE 'electronic'
    END AS object_label,

    -- Simulate bounding box coordinates
    UNIFORM(300, 500, RANDOM())::NUMBER AS xmax,
    UNIFORM(0, 200, RANDOM())::NUMBER AS xmin,
    UNIFORM(300, 500, RANDOM())::NUMBER AS ymax,
    UNIFORM(0, 200, RANDOM())::NUMBER AS ymin

FROM object_data,
     TABLE(GENERATOR(ROWCOUNT => 10000));


     
select * from resort_vision_inference limit 10;

CREATE OR REPLACE TABLE resort_camera_zones AS
SELECT * FROM VALUES
    ('CAM001', 'Spa Lobby',        'public',      1, TRUE,  '85620'),
    ('CAM002', 'Main Pool',        'public',      0, FALSE, '85620'),
    ('CAM003', 'Reception',        'public',      1, TRUE,  '85620'),
    ('CAM004', 'Casino Floor',     'restricted',  2, TRUE,  '85620'),
    ('CAM005', 'Villa Hallway',    'private',     1, TRUE,  '85620'),
    ('CAM006', 'Kids Club',        'public',      1, TRUE,  '85620'),
    ('CAM007', 'Lounge Bar',       'public',      1, TRUE,  '85620'),
    ('CAM008', 'Gym',              'public',      2, TRUE,  '85620')
AS t(camera_id, location_name, zone_type, floor_number, is_indoor, zip_code);



select * from resort_camera_zones;


CREATE OR REPLACE VIEW resort_bot_semantic_view AS
SELECT 
    vi.inference_timestamp,
    vi.camera_id,
    cz.location_name,
    cz.zone_type,
    cz.floor_number,
    cz.is_indoor,
    
    vi.object_type,
    vi.object_label,
    vi.is_valuable,
    vi.label_confidence_score,
    vi.file_name,
    vi.xmin,
    vi.xmax,
    vi.ymin,
    vi.ymax,

    gc.guest_id,
    gc.guest_name,
    gc.room_number,
    gc.vip_status,
    gc.guest_type,
    gc.loyalty_tier,
    gc.country,
    gc.booking_source

FROM resort_vision_inference vi
JOIN resort_camera_zones cz
  ON vi.camera_id = cz.camera_id

LEFT JOIN resort_guest_checkin gc
  ON vi.inference_timestamp BETWEEN gc.checkin_datetime AND gc.checkout_datetime;


create or replace view cam_weather_view as 
SELECT 
    v.inference_timestamp,
    v.object_type,
    v.is_valuable,
    v.file_name,
    cz.location_name,
    cz.zip_code,
    w.MAX_TEMPERATURE_AIR_2M_F,
    w.MIN_TEMPERATURE_AIR_2M_F,
    w.TOT_PRECIPITATION_IN,
    CASE
        WHEN w.TOT_PRECIPITATION_IN > 0.1 THEN 'rainy'
        WHEN w.MAX_TEMPERATURE_AIR_2M_F > 90 THEN 'hot'
        WHEN w.MIN_TEMPERATURE_AIR_2M_F < 50 THEN 'cold'
        WHEN w.MAX_WIND_SPEED_10M_MPH > 25 THEN 'windy'
        WHEN w.MAX_HUMIDITY_RELATIVE_2M_PCT > 80 THEN 'humid'
        ELSE 'clear'
    END AS derived_weather_condition
FROM resort_vision_inference v
JOIN resort_camera_zones cz
  ON v.camera_id = cz.camera_id
JOIN  global_weather__climate_data_for_bi.standard_tile.history_day w
  ON cz.zip_code = w.postal_code
  AND DATE(v.inference_timestamp) = w.DATE_VALID_STD;

CREATE OR REPLACE TABLE resort_lost_and_found AS
WITH objects AS (
    SELECT 
        ARRAY_CONSTRUCT('passport','purse','jewelry','watch','phone','wallet','backpack','laptop') AS items,
        ARRAY_CONSTRUCT('Spa Lobby','Main Pool','Reception','Casino Floor','Villa Hallway','Kids Club','Lounge Bar','Gym') AS zones
)
SELECT
    'LFR_' || LPAD(TO_VARCHAR(SEQ4()), 5, '0') AS report_id,
    'GUEST_' || LPAD(TO_VARCHAR(UNIFORM(0, 99999, RANDOM())), 5, '0') AS guest_id,
    DATEADD(HOUR, -UNIFORM(0, 24*90, RANDOM()), CURRENT_TIMESTAMP()) AS report_timestamp,

    -- Clean object_type and location
    TRIM(items[UNIFORM(0, 7, RANDOM())])::VARCHAR AS object_type,
    TRIM(zones[UNIFORM(0, 7, RANDOM())])::VARCHAR AS location_reported,

    -- Clean description from object/location
    INITCAP(
        TRIM(items[UNIFORM(0, 7, RANDOM())]) || ' lost near ' || 
        TRIM(zones[UNIFORM(0, 7, RANDOM())])
    ) AS description,

    -- Clean status with consistent output
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'reported'
        WHEN 2 THEN 'found'
        WHEN 3 THEN 'matched'
        ELSE 'returned'
    END AS status

FROM objects,
     TABLE(GENERATOR(ROWCOUNT => 2000));





CREATE OR REPLACE TABLE resort_event_unified_table AS 
WITH
-- 1. Base inference
base_inference AS (
  SELECT * FROM resort_vision_inference
),

-- 2. Camera metadata
camera_data AS (
  SELECT * FROM resort_camera_zones
),

-- ✅ 3. Guest match (filtered first, then joined safely)
guest_match AS (
  SELECT
    file_name,
    guest_id,
    guest_name,
    vip_status,
    room_number,
    guest_type,
    loyalty_tier,
    booking_source,
    country
  FROM (
    SELECT
      v.file_name,
      gc.guest_id,
      gc.guest_name,
      gc.vip_status,
      gc.room_number,
      gc.guest_type,
      gc.loyalty_tier,
      gc.booking_source,
      gc.country,
      ROW_NUMBER() OVER (
        PARTITION BY v.file_name
        ORDER BY gc.checkin_datetime DESC
      ) AS rn
    FROM resort_vision_inference v
    LEFT JOIN resort_guest_checkin gc
      ON v.inference_timestamp BETWEEN gc.checkin_datetime AND gc.checkout_datetime
  ) sub
  WHERE rn = 1
),

-- ✅ 4. Lost & Found match (1 per inference)
lost_found_match AS (
  SELECT
    file_name,
    report_id,
    report_timestamp,
    status AS lost_item_status,
    description AS lost_item_description
  FROM (
    SELECT
      v.file_name,
      lf.report_id,
      lf.report_timestamp,
      lf.status,
      lf.description,
      ROW_NUMBER() OVER (
        PARTITION BY v.file_name
        ORDER BY ABS(DATEDIFF('hour', lf.report_timestamp, v.inference_timestamp))
      ) AS rn
    FROM resort_vision_inference v
    JOIN resort_camera_zones cz ON v.camera_id = cz.camera_id
    JOIN resort_lost_and_found lf
      ON lf.object_type = v.object_type
     AND lf.location_reported = cz.location_name
     AND ABS(DATEDIFF('hour', lf.report_timestamp, v.inference_timestamp)) <= 12
  ) sub
  WHERE rn = 1
),

-- ✅ 5. Weather data deduplicated by ZIP & date
weather_data AS (
  SELECT DISTINCT
    postal_code,
    DATE_VALID_STD,
    MAX_TEMPERATURE_AIR_2M_F,
    MIN_TEMPERATURE_AIR_2M_F,
    TOT_PRECIPITATION_IN,
    MAX_WIND_SPEED_10M_MPH,
    MAX_HUMIDITY_RELATIVE_2M_PCT
  FROM global_weather__climate_data_for_bi.standard_tile.history_day
)

-- ✅ Final SELECT: clean 1:1 joins
SELECT 
    v.inference_timestamp,
    v.camera_id,
    v.object_type,
    v.object_label,
    v.is_valuable,
    v.label_confidence_score,
    v.file_name,
    v.xmin, v.xmax, v.ymin, v.ymax,

    cz.location_name,
    cz.zone_type,
    cz.floor_number,
    cz.is_indoor,
    cz.zip_code,

    g.guest_id,
    g.guest_name,
    g.vip_status,
    g.room_number,
    g.guest_type,
    g.loyalty_tier,
    g.booking_source,
    g.country,

    w.MAX_TEMPERATURE_AIR_2M_F AS temperature_max_f,
    w.MIN_TEMPERATURE_AIR_2M_F AS temperature_min_f,
    w.TOT_PRECIPITATION_IN AS total_precip_in,

    CASE
        WHEN w.TOT_PRECIPITATION_IN > 0.1 THEN 'rainy'
        WHEN w.MAX_TEMPERATURE_AIR_2M_F > 90 THEN 'hot'
        WHEN w.MIN_TEMPERATURE_AIR_2M_F < 50 THEN 'cold'
        WHEN w.MAX_WIND_SPEED_10M_MPH > 25 THEN 'windy'
        WHEN w.MAX_HUMIDITY_RELATIVE_2M_PCT > 80 THEN 'humid'
        ELSE 'clear'
    END AS derived_weather_condition,

    lf.report_id AS lost_report_id,
    lf.report_timestamp,
    lf.lost_item_status,
    lf.lost_item_description

FROM base_inference v
JOIN camera_data cz ON v.camera_id = cz.camera_id

LEFT JOIN guest_match g ON v.file_name = g.file_name
LEFT JOIN lost_found_match lf ON v.file_name = lf.file_name

LEFT JOIN weather_data w
  ON cz.zip_code = w.postal_code
 AND DATE(v.inference_timestamp) = w.DATE_VALID_STD;