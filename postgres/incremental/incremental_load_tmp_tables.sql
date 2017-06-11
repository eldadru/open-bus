\timing


--- Create all the temporary tables and load the data from the files into them
-- this stage is very similar to loading a non-incremental GTFS except for the table names and loading route stories
-- rather than stop times

\echo ********** importing agencies **********
DROP TABLE IF EXISTS tmp_agencies;
CREATE TABLE tmp_agencies
(
  a_id     INTEGER                NOT NULL,
  name     CHARACTER VARYING(100) NOT NULL,
  url      TEXT,
  timezone CHARACTER VARYING(20),
  lang     CHARACTER VARYING(2),
  phone    CHARACTER VARYING(20),
  fare_url CHARACTER VARYING(100)
);

\copy tmp_agencies from '/tmp/gtfs/agency.txt' DELIMITER ',' CSV HEADER NULL AS '';


\echo ********** importing routes **********
DROP TABLE IF EXISTS tmp_routes;
CREATE TABLE tmp_routes
(
  r_id        INTEGER NOT NULL,
  agency_id   INTEGER,
  short_name  CHARACTER VARYING(50),
  long_name   CHARACTER VARYING(255),
  route_desc  CHARACTER VARYING(10),
  route_type  INTEGER NOT NULL,
  route_color CHARACTER VARYING(6),
  CONSTRAINT tmp_routes_pkey PRIMARY KEY (r_id)
);

\copy tmp_routes from '/tmp/gtfs/routes.txt' DELIMITER ',' CSV HEADER NULL AS '';

\echo ********** importing stops **********
DROP TABLE IF EXISTS tmp_stops;
CREATE TABLE tmp_stops
(
  s_id           INTEGER NOT NULL,
  stop_code      INTEGER,
  name           CHARACTER VARYING(255),
  s_desc         CHARACTER VARYING(255),
  stop_lat       NUMERIC(10, 8),
  stop_lon       NUMERIC(10, 8),
  location_type  BOOLEAN,
  parent_station INTEGER,
  zone_id        CHARACTER VARYING(255),
  CONSTRAINT tmp_stops_pkey PRIMARY KEY (s_id)
);

\copy tmp_stops from '/tmp/gtfs/stops.txt' DELIMITER ',' CSV HEADER NULL AS '';

SELECT AddGeometryColumn('tmp_stops', 'point', 4326, 'POINT', 2);

UPDATE tmp_stops
SET point = ST_SetSRID(ST_MakePoint(cast(stop_lon AS DOUBLE PRECISION), cast(stop_lat AS DOUBLE PRECISION)), 4326)
WHERE stop_lon IS NOT NULL;

\echo ********** importing shapes **********
DROP TABLE IF EXISTS tmp_shapes;
CREATE TABLE tmp_shapes
(
  s_id        INTEGER       NOT NULL,
  pt_lat      NUMERIC(8, 6) NOT NULL,
  pt_lon      NUMERIC(8, 6) NOT NULL,
  pt_sequence INTEGER       NOT NULL
);

\copy tmp_shapes from '/tmp/gtfs/shapes.txt' DELIMITER ',' CSV HEADER NULL AS '';

CREATE INDEX tmp_shape_lines_shapes ON tmp_shape_lines USING GIST (shape);

\echo ********** converting to shape lines **********
DROP TABLE IF EXISTS tmp_shape_lines;
CREATE TABLE tmp_shape_lines AS
  (SELECT
     tmp.s_id,
     St_makeline(St_setsrid(St_makepoint(pt_lon, pt_lat), 4326)) AS shape
   FROM (SELECT *
         FROM tmp_shapes
         ORDER BY pt_sequence) AS tmp
   GROUP BY tmp.s_id
  );


\echo ********* importing route story summaries ************
DROP TABLE IF EXISTS tmp_route_stories;
CREATE TABLE IF NOT EXISTS tmp_route_stories
(
  rs_id     INTEGER NOT NULL,
  rs_hash   BIGINT  NOT NULL,
  rs_string TEXT,
  CONSTRAINT tmp_route_story_summaries_pkey PRIMARY KEY (rs_id)
);

\copy tmp_route_stories from '/tmp/gtfs/route_story_summaries.txt' DELIMITER ',' CSV HEADER NULL AS '';

\echo ********* importing route story stops ************
DROP TABLE IF EXISTS tmp_route_story_stops;
CREATE TABLE tmp_route_story_stops
(
  rs_id               INTEGER  NOT NULL,
  arrival_offset      SMALLINT NOT NULL,
  departure_offset    SMALLINT NOT NULL,
  stop_id             INTEGER  NOT NULL,
  stop_sequence       SMALLINT NOT NULL,
  drop_off_only       BOOLEAN,
  pickup_only         BOOLEAN,
  shape_dist_traveled INTEGER
);

\copy tmp_route_story_stops from '/tmp/gtfs/route_story_stops.txt' DELIMITER ',' CSV HEADER NULL AS '';

\echo ********** importing trips ********************
DROP TABLE IF EXISTS tmp_trips;
CREATE TABLE tmp_trips
(
  route_id     INTEGER,
  service_id   INTEGER,
  trip_id      CHARACTER VARYING(50) NOT NULL,
  direction_id INTEGER,
  shape_id     INTEGER
);

\copy tmp_trips from '/tmp/gtfs/trips.txt' DELIMITER ',' CSV HEADER NULL AS '';

\echo ********* importing trip route stories ************
DROP TABLE IF EXISTS tmp_trip_route_stories;
CREATE TABLE tmp_trip_route_stories
(
  trip_id        CHARACTER VARYING(50) NOT NULL,
  rs_id          INTEGER               NOT NULL,
  departure_time TIME                  NOT NULL
);

\copy tmp_trip_route_stories from '/tmp/gtfs/trip_route_stories.txt' DELIMITER ',' CSV HEADER NULL AS '';

\echo ********** importing calendar (services) ****************
DROP TABLE IF EXISTS tmp_calendar;
CREATE TABLE tmp_calendar
(
  s_id INTEGER NOT NULL,
  sunday     BOOLEAN,
  monday     BOOLEAN,
  tuesday    BOOLEAN,
  wednesday  BOOLEAN,
  thursday   BOOLEAN,
  friday     BOOLEAN,
  saturday   BOOLEAN,
  start_date DATE,
  end_date   DATE,
  CONSTRAINT tmp_calendar_pk PRIMARY KEY (s_id)
);

\copy tmp_calendar from '/tmp/gtfs/calendar.txt' DELIMITER ',' CSV HEADER NULL AS '';


DROP table if EXISTS  tmp_service_days;
CREATE TABLE tmp_service_days (
  s_id        INTEGER,
  day_of_week SMALLINT,
  CONSTRAINT tmp_service_days_pk1 PRIMARY KEY (s_id, day_of_week)
);

