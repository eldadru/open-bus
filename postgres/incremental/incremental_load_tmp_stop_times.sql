\timing

\echo ****** Loading stop times tmp *********

DROP TABLE IF EXISTS tmp_stop_times;
CREATE TABLE tmp_stop_times
(
  trip_id             CHARACTER VARYING(50),
  arrival_time        CHARACTER VARYING(8),
  departure_time      CHARACTER VARYING(8),
  stop_id             INTEGER,
  stop_sequence       INTEGER,
  drop_off_only       BOOLEAN,
  pickup_only         BOOLEAN,
  shape_dist_traveled INTEGER
);

\copy tmp_stop_times from '/tmp/gtfs/stop_times.txt' DELIMITER ',' CSV HEADER;

CREATE INDEX tmp_stop_times_trip_id
  ON tmp_stop_times
  (trip_id, stop_sequence);

