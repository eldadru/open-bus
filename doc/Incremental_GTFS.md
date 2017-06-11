# Incremental GTFS

## The Need

We want to build up a database with all the scheduling data over the months and years to come. This will allow  us:

1. To see how shceduling changes over time (rise and fall in number of bus routes and planned trips, for example).
2. Always match real-time data with the correct scheduling data. 

GTFS is published by MoT everyday, with data for the next 60 days. You might think it means we only need to load a GTFS file every 60 days. However, changes often occur within the period. In today's file a certain trip might be scheduled for two weeks from now, but by the time the date arrive the trip will be canceled. So the "detemining" GTFS for every day is the one published that day. 

Therefore we need to load a file every day (ideally), and update the database with the new and changed data.

## Highlevel view

TBD

## Added benefits

In addition to incrementality, we are also using the import process to do several processing steps in the data which make it more useful for our purposes. These include:

* Renaming fields and dropping some low value fields. 
* Routes: splitting the `route_description` field into route_code  (makat), direction and alternative fields.
* Stops: 
* * extrating address, town, platform & floor from stop description.
* * replacing stop_lat and stop_long fields with Postgis point field. 
* Shapes: 
* * replacing with shape_lines table, which represents the shape as a Postgis linestring field. 
  * **Planned**: simplifying the shapes using PostGIS ST_Simplify (Douglas-Peucker algorithm). The original shapes are over-sampled.
* * **Planned**:  adding an encoded_shape field for easy presentation using Google maps.
* Route stories: route stories are generated, replacing the stop_times table.
    ​


## Schema

![Incremental GTFS schema](https://github.com/hasadna/open-bus/blob/master/doc/incremental_gtfs.png)

### igtfs_files
Lists the files imported:
* `gtfs_date` - date when the file was produced. This is calculated as the minimum value of `start_date` in the calendar file.
* `file_size` - size of the GTFS zip file in bytes. Meant to help finding the original file if needed...
* `imported_on` - time stamp of the import.

### igtfs_agencies

Agencies (operators) list, based on GTFS agency.txt:

* `a_id` - surrogate key (auto generated) 
* `orig_id` - source: GTFS `agency.agency_id`
* `name` - source: GTFS `agency.agency_name` 
* `active_from` - first GTFS file where this agency original id & name was seen. References `igtfs_files.gtfs_date`
* `active_until` - last GTFS file where this agency original id & name was seen. References `igtfs_files.gtfs_date`. `Null` if agency is currently active. 

### igtfs_routes

Bus routes ("lines") line, based on GTFS routes.txt:

* `r_id` - surrogate key (auto generated)
* `agency_id` - references `agencies.a_id`
* `label` - Bus line number as displayed on the bus.  Source: GTFS `routes.route_short_name`.
* `name` - A descriptive name of the route, composed of start and end stops. Source: GTFS `routes.route_long_name`.
* `code` - The MoT route code (מק"ט). Source: extracted from GTFS `routes.route_description`.
* `direction` - Direction code from MoT (1, 2 or 3 for outbound, inbound or circular). Source: extracted from GTFS `routes.route_description`
* `alternative` - Alternative code from MoT (see [this blog pos](http://israelbikebus.blogspot.co.uk/2015/04/blog-post_28.html)t to understand the concept of an alternative.) Source: extracted from GTFS `routes.route_description`.
* `route_type` - Current available values or 2 for trains and 3 for buses. Source: GTFS `routes.route_type` field.
* `active_from` - first GTFS file where this (agency, label, code, direction & alternative) combination was seen. References `igtfs_files.gtfs_date`
* `active_until` - last GTFS file where this (agency, label, code, direction & alternative) combination was seen. References `igtfs_files.gtfs_date`. `Null` if route is currently active. 

### igtfs_stops

Bus stops and train stations, based on GTFS stops.txt:

* `s_id` - surrogate key (auto generated)
* `code` - stop code. Source: GTFS `stops.stop_code`. 
* `name` - stop name. Source: GTFS `stops.stop_name`.
* `point` - WGS84 coordinate of the stop using PostGIS geometry(point) type. Source: GTFS `stops.stop_lat` and `stop.stop_lon` fields.
* `is_central_station` - is this a terminal containing multiple individual stops. Source: GTFS `stops.location_type`.
* `zone_id` - taarif zone id. Source: GTFS `stops.zone_id`
* `address` - stop's street address. Source: extracted from GTFS `stops.stop_description`
* `town` - stop's town or village. Source: extracted from `stops.stop_description`.
* `platform` - stop's platform. Non-empty only for stops inside terminals. Source: extracted from `stops.stop_description`.
* `floor` - stop's floor in terminal. Non-empty only for stops inside multi-level terminals, like Jerusalem and Tel-Aviv central stations.  Source: extracted from `stops.stop_description`.
* `active_from` - first GTFS file where this (code, name, is_central_station, platform, floor, point) combination was seen. References `igtfs_files.gtfs_date`
* `active_until` - last GTFS file where this (code, name, is_central_station, platform, floor, point) combination was seen. References `igtfs_files.gtfs_date`. `Null` if stop  is currently active. 

### igtfs_shape_lines

Geometry of the bus route, based on GTFS shapes.txt:

* `s_id`: surrogate key (auto generated)
* `shape`: WGS84 polyline using Postgis geometry(linestring) type. Source: GTFS `shapes.shape_pt_lat`, `shapes.shape_pt_lon` and `shapes.shape_pt_seq`.


### igtfs_route_story_summaries

Route stories are described in details in [the docstring of the module that computes them](https://github.com/hasadna/open-bus/blob/master/gtfs/parser/route_stories.py). The tl;dr is that they are a way to compress the stop_times table. 

route_story_summaries table contains a single per route story.

* `rs_id` - surrogate key (auto generated)
* `rs_hash` - an integer hash of the route story stops and times. This allows for fast comparisons and joins on route stories. Generated by the script that creates the route stories.
* `rs_string` - a string representation of the route story. A dash-separated list of stop codes and stop arrival times. Allows for fast comparison and joins of route stories. Generated by the script that creates the route stories.
* `active_from` - first GTFS file where this (rs_hash, rs_string) combination was seen. References `igtfs_files.gtfs_date`
* `active_until` - last GTFS file where this (rs_hash, rs_string)combination was seen. References `igtfs_files.gtfs_date`. `Null` if route story is currently active. 

### igtfs_route_story_stops

This is a detailed view of the route stories, where each record represents one stop in the story. 

* `rs_id` - references `igtfs_route_story_summaries.rs_id`
* `stop_sequence` - the sequence number of the stop in this story, allowing for correct ordering of stops. Integer starting from 1.
* `arrival_offset` - time of arrival to the stop, in minutes since departure from first stop.
* `departure_offset` - time of departure from the stop, in minutes since departure from the first stop. In practice we have found `arrival_offset` always equals `departure_offset`.
* `stop_id` - references `igtfs_stops.s_id`
* `drop_off_only` - whether the bus only drops off at this stop (you can't board the bus).
* `pickup_only` - whether the bus only picks up passenger at this stop (you can't leave!).
* `shape_dist_traveled` - the distance in meters, traveled by the bus from the first stop to this stop. Source: GTFS `stop_times.shape_dist_traveled` (we do not verify this calculation).
* `shape_dist_ratio` - `shape_dist_traveled` of this stop / `shape_dist_traveled` of last stop in this route story.

### igtfs_trips

The cherry in the cake! 

This is based on data from GTFS trips.txt, but is structured differently. Every record represent a single bus trip departing in a specific date and time.

* `t_id`: surrogate key (auto incremented)
* `orig_id`: the left part of the original trip id (without the date part). Source: GTFS `trips.trip_id`
* `route_id`: references `igtfs_routes.r_id`
* `route_story_id`: references `igtfs_route_story_summaries.rs_id`
* `departure_time`: time stamp of departure from first stop (date and time of day). Based on data extracted from GTFS `calendar.txt` and `stop_times.txt`
* `shape_id`: references `igtfs_shape_lines.s_id`

## Process

TBD
