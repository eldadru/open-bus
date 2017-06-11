CREATE TABLE IF NOT EXISTS igtfs_files
(
  file_date         DATE      NOT NULL, -- earliest start_date in calendar file
  file_size         BIGINT,
  imported_on       TIMESTAMP NOT NULL, -- timestamp of record insert
  CONSTRAINT igtfs_files_pkey PRIMARY KEY (file_date)
);

CREATE TABLE IF NOT EXISTS igtfs_agencies
(
  a_id         SERIAL,
  orig_id      INTEGER                NOT NULL, -- original agency id from gtfs file
  name         CHARACTER VARYING(100) NOT NULL,
  active_from  DATE                   NOT NULL REFERENCES igtfs_files (file_date),
  active_until DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_agency_pkey PRIMARY KEY (a_id)
);

CREATE TABLE IF NOT EXISTS igtfs_routes
(
  r_id         SERIAL,
  agency_id    INTEGER REFERENCES igtfs_agencies (a_id),
  label        CHARACTER VARYING(50),
  name         CHARACTER VARYING(255),
  code         INTEGER,
  direction    SMALLINT,
  alternative  VARCHAR(2),
  route_type   SMALLINT NOT NULL,
  active_from  DATE    NOT NULL REFERENCES igtfs_files (file_date),
  active_until DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_routes_pkey PRIMARY KEY (r_id)
);

CREATE INDEX IF NOT EXISTS igtfs_routes_core_fields
  ON igtfs_routes
  (orig_id, agency_id, short_name, route_desc);


CREATE TABLE IF NOT EXISTS igtfs_stops
(
  s_id               SERIAL,
  code               INTEGER NOT NULL,
  name               CHARACTER VARYING(255),
  is_central_station BOOLEAN,
  zone_id            CHARACTER VARYING(5),
  address            CHARACTER VARYING(50),
  town               CHARACTER VARYING(50),
  platform           CHARACTER VARYING(4),
  floor              CHARACTER VARYING(10),
  active_from        DATE    NOT NULL REFERENCES igtfs_files (file_date),
  active_until       DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_stops_pkey PRIMARY KEY (s_id)
);

SELECT AddGeometryColumn('igtfs_stops', 'point', 4326, 'POINT', 2);

CREATE TABLE IF NOT EXISTS igtfs_shape_lines
(
  s_id SERIAL,
  CONSTRAINT igtfs_shape_lines_pkey PRIMARY KEY (s_id)
);

SELECT AddGeometryColumn('igtfs_shape_lines', 'shape', 4326, 'LINESTRING', 2);

CREATE INDEX igtfs_shape_lines_shapes ON igtfs_shape_lines USING GIST (shape);



CREATE TABLE IF NOT EXISTS igtfs_route_stories
(
  rs_id        SERIAL,
  rs_hash      BIGINT NOT NULL,
  rs_string    TEXT,
  active_from  DATE   NOT NULL REFERENCES igtfs_files (file_date),
  active_until DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_route_story_summaries_pkey PRIMARY KEY (rs_id)
);

CREATE INDEX ON igtfs_route_stories (rs_hash, rs_string);


CREATE TABLE IF NOT EXISTS igtfs_route_story_stops
(
  rs_id               INTEGER  NOT NULL REFERENCES igtfs_route_stories(rs_id),
  stop_sequence       SMALLINT NOT NULL,
  arrival_offset      SMALLINT NOT NULL,
  departure_offset    SMALLINT NOT NULL,
  stop_id             INTEGER  NOT NULL REFERENCES igtfs_stops (s_id),
  drop_off_only       BOOLEAN,
  pickup_only         BOOLEAN,
  shape_dist_traveled INTEGER,
  shape_dist_ratio    REAL,
  CONSTRAINT igtfs_route_stories_pkey PRIMARY KEY (rs_id, stop_sequence)
);

CREATE TABLE igtfs_trips
(
  t_id           SERIAL, -- auto increment
  orig_id        INTEGER NOT NULL ,
  route_id       INTEGER REFERENCES igtfs_routes (r_id),
  route_story_id INTEGER REFERENCES igtfs_route_stories (rs_id),
  shape_id       INTEGER REFERENCES igtfs_shape_lines (s_id),
  departure_time TIMESTAMP NOT NULL,        -- date & time of departure
  CONSTRAINT igtfs_trips_pkey PRIMARY KEY (t_id)
);

