\echo Inserting to igtfs_files
INSERT INTO igtfs_files (file_date, file_size, imported_on)
VALUES(:'gtfs_date', :file_size, current_timestamp);


---- agencies

\echo Inserting new agencies
INSERT INTO igtfs_agencies (orig_id, name, active_from, active_until)
  SELECT
    tmp_agencies.a_id,
    tmp_agencies.name,
    :'gtfs_date',
    NULL
  FROM tmp_agencies
    LEFT JOIN igtfs_agencies
      ON tmp_agencies.a_id = igtfs_agencies.orig_id
         AND tmp_agencies.name = igtfs_agencies.name
  WHERE igtfs_agencies.a_id ISNULL AND igtfs_agencies.active_until ISNULL
  ORDER BY tmp_agencies.a_id;

\echo Closing old agencies
UPDATE igtfs_agencies
SET active_until = :'gtfs_date'
FROM (
  SELECT igtfs_agencies.a_id
  FROM igtfs_agencies
    LEFT JOIN tmp_agencies
      ON igtfs_agencies.orig_id = tmp_agencies.a_id
        AND igtfs_agencies.name = tmp_agencies.name
  WHERE tmp_agencies.a_id ISNULL AND igtfs_agencies.active_until ISNULL ) tmp
WHERE tmp.a_id = igtfs_agencies.a_id;

----- routes

\echo Fix agency ids in routes
UPDATE tmp_routes
SET agency_id = igtfs_agencies.a_id
FROM igtfs_agencies
WHERE tmp_routes.agency_id = igtfs_agencies.orig_id
      AND igtfs_agencies.active_until ISNULL;

\echo Insert new routes
INSERT INTO igtfs_routes (agency_id, label, name, code, direction, alternative, route_type, active_from, active_until)
  SELECT
    tmp_routes.agency_id,
    tmp_routes.short_name,
    tmp_routes.long_name,
    split_part(tmp_routes.route_desc, '-', 1) :: INTEGER,
    split_part(tmp_routes.route_desc, '-', 2) :: SMALLINT,
    split_part(tmp_routes.route_desc, '-', 3),
    tmp_routes.route_type,
    :'gtfs_date',
    NULL
  FROM tmp_routes
    LEFT JOIN igtfs_routes
      ON tmp_routes.agency_id = igtfs_routes.agency_id
         AND coalesce(tmp_routes.short_name, '') = coalesce(igtfs_routes.label, '')
         AND split_part(tmp_routes.route_desc, '-', 1) :: INTEGER = igtfs_routes.code
         AND split_part(tmp_routes.route_desc, '-', 2) :: SMALLINT = igtfs_routes.direction
         AND split_part(tmp_routes.route_desc, '-', 3) = igtfs_routes.alternative
  WHERE igtfs_routes.r_id ISNULL
        AND igtfs_routes.active_until ISNULL;

\echo Closing old routes
UPDATE igtfs_routes
SET active_until = :'gtfs_date'
FROM (
       SELECT igtfs_routes.r_id
       FROM igtfs_routes
         LEFT JOIN tmp_routes
           ON tmp_routes.agency_id = igtfs_routes.agency_id
              AND COALESCE(tmp_routes.short_name, '') = COALESCE(igtfs_routes.label, '')
              AND split_part(tmp_routes.route_desc, '-', 1) :: INTEGER = igtfs_routes.code
              AND split_part(tmp_routes.route_desc, '-', 2) :: SMALLINT = igtfs_routes.direction
              AND split_part(tmp_routes.route_desc, '-', 3) = igtfs_routes.alternative
       WHERE tmp_routes.r_id ISNULL
             AND igtfs_routes.active_until ISNULL) tmp
WHERE tmp.r_id = igtfs_routes.r_id;

----- stops
-- note we don't check whether stops are in use...

\echo Inserting new stops
INSERT INTO igtfs_stops (code, name,  is_central_station, zone_id, address, town, platform, floor, active_from, active_until, point)
  SELECT
    tmp_stops.stop_code,
    tmp_stops.name,
    tmp_stops.location_type,
    tmp_stops.zone_id,
    left(trim(split_part(tmp_stops.s_desc, ':', 2)), -4),
    left(trim(split_part(tmp_stops.s_desc, ':', 3)), -5),
    left(trim(split_part(tmp_stops.s_desc, ':', 4)), -4),
    trim(split_part(tmp_stops.s_desc, ':', 5)),
    :'gtfs_date',
    NULL,
    tmp_stops.point
  FROM tmp_stops
    LEFT JOIN igtfs_stops
      ON tmp_stops.stop_code = igtfs_stops.code
         AND tmp_stops.name = igtfs_stops.name
         AND tmp_stops.location_type = igtfs_stops.is_central_station
         AND left(trim(split_part(tmp_stops.s_desc, ':', 4)), -4) = igtfs_stops.platform
         AND trim(split_part(tmp_stops.s_desc, ':', 5)) = igtfs_stops.floor
         AND ST_DistanceSphere(tmp_stops.point, igtfs_stops.point) < 10
  WHERE igtfs_stops.code ISNULL AND igtfs_stops.active_until ISNULL;

\echo Closing unused stops
UPDATE igtfs_stops
SET active_until = :'gtfs_date'
FROM (
       SELECT igtfs_stops.s_id
       FROM igtfs_stops
         LEFT JOIN tmp_stops
           ON tmp_stops.stop_code = igtfs_stops.code
              AND tmp_stops.name = igtfs_stops.name
              AND tmp_stops.location_type = igtfs_stops.is_central_station
              AND LEFT(TRIM(split_part(tmp_stops.s_desc, ':', 4)), -4) = igtfs_stops.platform
              AND TRIM(split_part(tmp_stops.s_desc, ':', 5)) = igtfs_stops.floor
              AND ST_Distance(tmp_stops.point, igtfs_stops.point) < 10
       WHERE tmp_stops.stop_code ISNULL AND igtfs_stops.active_until ISNULL) tmp
WHERE tmp.s_id = igtfs_stops.s_id;

\echo Updating tmp_stops with db_id
ALTER TABLE tmp_stops
  ADD COLUMN db_id INTEGER;

UPDATE tmp_stops
SET db_id = igtfs_stops.s_id
FROM igtfs_stops
WHERE igtfs_stops.code = tmp_stops.stop_code
      AND tmp_stops.location_type = igtfs_stops.is_central_station
      AND left(trim(split_part(tmp_stops.s_desc, ':', 4)), -4) = igtfs_stops.platform
      AND trim(split_part(tmp_stops.s_desc, ':', 5)) = igtfs_stops.floor
      AND igtfs_stops.active_until ISNULL;

----- shapes
-- \delete duplicates from tmp_shape_lines
-- DELETE FROM tmp_shape_lines
-- WHERE exists(SELECT 1
--              FROM tmp_shape_lines t2
--              WHERE ST_equals(t2.shape, tmp_shape_lines.shape)
--                    AND t2.s_id > tmp_shape_lines.s_id);

\echo Inserting new shapes
INSERT INTO igtfs_shape_lines (shape)
  SELECT tmp_shape_lines.shape
  FROM tmp_shape_lines
    LEFT JOIN igtfs_shape_lines
      ON ST_equals(tmp_shape_lines.shape, igtfs_shape_lines.shape)
  WHERE igtfs_shape_lines.shape IS NULL;


\echo Updating tmp_shapes with db_id
ALTER TABLE tmp_shape_lines
  ADD COLUMN db_id INTEGER;

UPDATE tmp_shape_lines
SET db_id = igtfs_shape_lines.s_id
FROM igtfs_shape_lines
WHERE
  ST_equals(tmp_shape_lines.shape, igtfs_shape_lines.shape);

------------- route stories ------------------------------
-- this assume route_story_stops.txt, route_story_summaries.txt and trip_to_route_story.txt exist

\echo Inserting new route stories
INSERT INTO igtfs_route_stories (rs_hash, rs_string, active_from, active_until)
  SELECT
    tmp_route_stories.rs_hash,
    tmp_route_stories.rs_string,
    :'gtfs_date',
    NULL
  FROM tmp_route_stories
    LEFT JOIN igtfs_route_stories
      ON tmp_route_stories.rs_hash = igtfs_route_stories.rs_hash
         AND tmp_route_stories.rs_string = igtfs_route_stories.rs_string
  WHERE igtfs_route_stories.rs_id ISNULL
        AND igtfs_route_stories.active_until ISNULL;

\echo Closing old route stories
UPDATE igtfs_route_stories
SET active_until = :'gtfs_date'
FROM (
       SELECT igtfs_route_stories.rs_id
       FROM igtfs_route_stories
         LEFT JOIN tmp_route_stories
           ON tmp_route_stories.rs_hash = igtfs_route_stories.rs_hash
              AND tmp_route_stories.rs_string = igtfs_route_stories.rs_string
       WHERE tmp_route_stories.rs_id ISNULL
             AND igtfs_route_stories.active_until ISNULL
     ) tmp
WHERE tmp.rs_id = igtfs_route_stories.rs_id;

-- load the route story stops

\echo Fixing stop ids in tmp_route_story_stops
UPDATE tmp_route_story_stops
SET stop_id = tmp_stops.db_id
FROM tmp_stops
WHERE tmp_stops.s_id = tmp_route_story_stops.stop_id;

\echo Inserting route story stops for the new route stories
INSERT INTO igtfs_route_story_stops (rs_id, arrival_offset, departure_offset, stop_id, stop_sequence, drop_off_only, pickup_only, shape_dist_traveled)
  SELECT
    igtfs_route_stories.rs_id,
    tmp_route_story_stops.arrival_offset,
    tmp_route_story_stops.departure_offset,
    tmp_route_story_stops.stop_id,
    tmp_route_story_stops.stop_sequence,
    tmp_route_story_stops.drop_off_only,
    tmp_route_story_stops.pickup_only,
    tmp_route_story_stops.shape_dist_traveled
  FROM tmp_route_story_stops
    JOIN tmp_route_stories
      ON tmp_route_story_stops.rs_id = tmp_route_stories.rs_id
    JOIN igtfs_route_stories
      ON tmp_route_stories.rs_hash = igtfs_route_stories.rs_hash
         AND tmp_route_stories.rs_string = igtfs_route_stories.rs_string
  WHERE igtfs_route_stories.active_from = :'gtfs_date';

\echo Setting shape_dist_ratio for new route story stops
UPDATE igtfs_route_story_stops
SET shape_dist_ratio = (1.0 * shape_dist_traveled) /
                       (SELECT MAX(shape_dist_traveled)
                        FROM igtfs_route_story_stops a
                        WHERE a.rs_id = igtfs_route_story_stops.rs_id)
WHERE igtfs_route_story_stops.shape_dist_ratio ISNULL;

------------------------ finally, trips! -----------------------

\echo Fixing route_id in tmp_trips
UPDATE tmp_trips
SET route_id = tmp.igtfs_id
FROM (
       SELECT igtfs_routes.r_id as igtfs_id, tmp_routes.r_id as tmp_id
       FROM igtfs_routes
         JOIN tmp_routes
           ON tmp_routes.agency_id = igtfs_routes.agency_id
              AND COALESCE(tmp_routes.short_name, '') = COALESCE(igtfs_routes.label, '')
              AND split_part(tmp_routes.route_desc, '-', 1) :: INTEGER = igtfs_routes.code
              AND split_part(tmp_routes.route_desc, '-', 2) :: SMALLINT = igtfs_routes.direction
              AND split_part(tmp_routes.route_desc, '-', 3) = igtfs_routes.alternative
         JOIN tmp_trips
           on tmp_trips.route_id = tmp_routes.r_id
       WHERE igtfs_routes.active_until ISNULL) tmp
WHERE tmp.tmp_id = tmp_trips.route_id;



\echo Fixing shape_id in tmp_trips
UPDATE tmp_trips
SET shape_id = tmp_shape_lines.db_id
FROM tmp_shape_lines
WHERE tmp_shape_lines.s_id = tmp_trips.shape_id;

\echo Setting set rs_id and departure time in tmp_trips
ALTER TABLE tmp_trips
  ADD COLUMN rs_id INTEGER,
  ADD COLUMN departure_time TIME;

UPDATE tmp_trips
SET rs_id = db_id, departure_time = tmp.departure_time
FROM (
       SELECT
         tmp_trip_route_stories.trip_id,
         igtfs_route_stories.rs_id AS db_id,
         tmp_trip_route_stories.departure_time
       FROM tmp_trip_route_stories
         JOIN tmp_route_stories
           ON tmp_trip_route_stories.rs_id = tmp_route_stories.rs_id
         JOIN igtfs_route_stories
           ON tmp_route_stories.rs_hash = igtfs_route_stories.rs_hash
              AND tmp_route_stories.rs_string = igtfs_route_stories.rs_string
       WHERE igtfs_route_stories.active_until ISNULL
     ) tmp
WHERE tmp.trip_id = tmp_trips.trip_id;

-- decompress the tmp_trips table
DELETE FROM tmp_service_days;
INSERT INTO tmp_service_days (s_id, day_of_week)
  SELECT *
  FROM
    (SELECT
       s_id,
       0 as day
     FROM tmp_calendar
     WHERE sunday = TRUE
     UNION ALL
     SELECT
       s_id,
       1 as day
     FROM tmp_calendar
     WHERE monday = TRUE
     UNION ALL
     SELECT
       s_id,
       2
     FROM tmp_calendar
     WHERE tuesday = TRUE
     UNION ALL
     SELECT
       s_id,
       3
     FROM tmp_calendar
     WHERE wednesday = TRUE
     UNION ALL
    SELECT
    s_id,
    4
    FROM tmp_calendar
    WHERE thursday = TRUE
     UNION ALL
    SELECT
    s_id,
    5
    FROM tmp_calendar
    WHERE friday = TRUE
     UNION ALL
     SELECT
       s_id,
       6
     FROM tmp_calendar
     WHERE saturday = TRUE
    ) T;


-- add all trips active on the start date
\echo Inserting trips for today
INSERT INTO igtfs_trips (orig_id, route_id, route_story_id, shape_id, departure_time)
  (
    SELECT
      split_part(trip_id, '_', 1)::int,
      route_id,
      rs_id,
      shape_id,
      (:'gtfs_date'::date + departure_time)
    FROM tmp_trips
      JOIN tmp_calendar
        ON tmp_trips.service_id = tmp_calendar.s_id
      JOIN tmp_service_days
        ON tmp_service_days.s_id=tmp_calendar.s_id
    WHERE tmp_calendar.start_date <= :'gtfs_date'
    AND tmp_calendar.end_date >= :'gtfs_date'
    AND extract(DOW FROM DATE :'gtfs_date') = tmp_service_days.day_of_week);



-- INSERT INTO igtfs_trips (orig_id, route_id, route_story_id, shape_id, departure_time)
--   (
--     SELECT
--       split_part(trip_id, '_', 1)::int,
--       route_id,
--       rs_id,
--       shape_id,
--       (:'gtfs_date'::date + departure_time)
--     FROM tmp_trips
--       JOIN tmp_calendar
--         ON tmp_trips.service_id = tmp_calendar.s_id
--       JOIN tmp_service_days
--         ON tmp_service_days.s_id=tmp_calendar.s_id
--     WHERE tmp_calendar.start_date <= :'gtfs_date'
--     AND tmp_calendar.end_date >= :'gtfs_date'
--     AND extract(DOW FROM DATE :'gtfs_date') = tmp_service_days.day_of_week
-- UNION ALL
--     SELECT
--      split_part(trip_id, '_', 1)::int,
--      route_id,
--      rs_id,
--      shape_id,
--      ((:'gtfs_date'::date + 1) + departure_time)
--    FROM tmp_trips
--      JOIN tmp_calendar
--        ON tmp_trips.service_id = tmp_calendar.s_id
--      JOIN tmp_service_days
--        ON tmp_service_days.s_id=tmp_calendar.s_id
--    WHERE tmp_calendar.start_date <= :'gtfs_date'::date + 1
--       AND tmp_calendar.end_date >= :'gtfs_date'::date + 1
--       AND extract(DOW FROM DATE :'gtfs_date'::date + 1) = tmp_service_days.day_of_week
-- UNION ALL
--     SELECT
--     split_part(trip_id, '_', 1)::int,
--     route_id,
--     rs_id,
--     shape_id,
--     ((:'gtfs_date'::date + 2) + departure_time)
--   FROM tmp_trips
--     JOIN tmp_calendar
--       ON tmp_trips.service_id = tmp_calendar.s_id
--     JOIN tmp_service_days
--       ON tmp_service_days.s_id=tmp_calendar.s_id
--   WHERE tmp_calendar.start_date <= :'gtfs_date'::date + 2
--       AND tmp_calendar.end_date >= :'gtfs_date'::date + 2
--       AND extract(DOW FROM DATE :'gtfs_date'::date + 2) = tmp_service_days.day_of_week
-- UNION ALL
--   SELECT
--     split_part(trip_id, '_', 1)::int,
--     route_id,
--     rs_id,
--     shape_id,
--     ((:'gtfs_date'::date + 3) + departure_time)
--   FROM tmp_trips
--     JOIN tmp_calendar
--       ON tmp_trips.service_id = tmp_calendar.s_id
--     JOIN tmp_service_days
--       ON tmp_service_days.s_id=tmp_calendar.s_id
--   WHERE tmp_calendar.start_date <= :'gtfs_date'::date + 3
--   AND tmp_calendar.end_date >= :'gtfs_date'::date + 3
--     AND extract(DOW FROM DATE :'gtfs_date'::date + 3) = tmp_service_days.day_of_week
-- UNION ALL
--       SELECT
--         split_part(trip_id, '_', 1)::int,
--         route_id,
--         rs_id,
--         shape_id,
--         ((:'gtfs_date'::date + 4) + departure_time)
--       FROM tmp_trips
--         JOIN tmp_calendar
--           ON tmp_trips.service_id = tmp_calendar.s_id
--         JOIN tmp_service_days
--           ON tmp_service_days.s_id=tmp_calendar.s_id
--       WHERE tmp_calendar.start_date <= :'gtfs_date'::date + 4
--       AND tmp_calendar.end_date >= :'gtfs_date'::date + 4
--     AND extract(DOW FROM DATE :'gtfs_date'::date + 4) = tmp_service_days.day_of_week
-- UNION ALL
--       SELECT
--         split_part(trip_id, '_', 1)::int,
--         route_id,
--         rs_id,
--         shape_id,
--         ((:'gtfs_date'::date + 5) + departure_time)
--       FROM tmp_trips
--         JOIN tmp_calendar
--           ON tmp_trips.service_id = tmp_calendar.s_id
--         JOIN tmp_service_days
--           ON tmp_service_days.s_id=tmp_calendar.s_id
--       WHERE tmp_calendar.start_date <= :'gtfs_date'::date + 5
--       AND tmp_calendar.end_date >= :'gtfs_date'::date + 5
--     AND extract(DOW FROM DATE :'gtfs_date'::date + 5) = tmp_service_days.day_of_week
-- UNION ALL
--       SELECT
--         split_part(trip_id, '_', 1)::int,
--         route_id,
--         rs_id,
--         shape_id,
--         ((:'gtfs_date'::date + 6) + departure_time)
--       FROM tmp_trips
--         JOIN tmp_calendar
--           ON tmp_trips.service_id = tmp_calendar.s_id
--         JOIN tmp_service_days
--           ON tmp_service_days.s_id=tmp_calendar.s_id
--       WHERE tmp_calendar.start_date <= :'gtfs_date'::date + 6
--       AND tmp_calendar.end_date >= :'gtfs_date'::date + 6
--     AND extract(DOW FROM DATE :'gtfs_date'::date + 6) = tmp_service_days.day_of_week);
--
