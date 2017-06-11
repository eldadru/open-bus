"""
Route stories: what they are and what they are good for

Background:
An intuitive model for a public transport would include the following objects:

* A bus line (e.g. bus line 189). In the GTFS this is called Route. The GTFS routes table has 
  separate records for each direction and alternative (halufa).
* A trip (e.g. 189 leaving terminal X on 2016-06-29 at 13:55). In practice most trips
  repeat every week or even every day. To reduce duplicates, a trip record in the GTFS doesn't 
  have a single date, but a start & end date and days of week.
* For each trip we would need the list of stops, and the arrival time to each stop. For example
  the Sunday 8:55 trip of 189, starts at stop X at time 8:55, arrive at stop Y at 8:56, arrives
  at stop z at 8:58 etc. This is implemented in the GTFS is stop_times table. 
  
What's the problem?
Since every trip has its own list of stops, the GTFS allow each trip of a route to be different.
So 189 of 8:55 could go from Tel-Aviv to Holon and 189 of 9:06 can go from Petah-Tikva to Azor.

In pracrice in Israel, each bus line has a fixed route (stop sequence). If a bus line has two 
routes (e.g. some trips go through a certain neighborhood and others don't) - the line will 
have to separate route records (halufot). The only reason for two different stop sequences
is if during the two months covered by the GTFS, there is a planned changed in the route
(so one stop sequence will be valid say until June 30th, and then a different starting July 1st).

And it gets better:

Not only the stop sequence is fixed, the arrival times to each stop are fixed. That is,
if the 08:00 trip arrives at stop x at 8:19, the 13:00 trip will arrive at stop x at 13:19.
There is not effort to publish (or probably even plan) a reasonable schedule that 
takes into account peak traffic.

Here comes in a new object, Route Story. A route story is a list of stops, and the time offset
of the arrival to the stop, compared to the beginning of the route.

E.g.:

    stop_times				route_story
    stop 1, 8:15			stop 1, 0
    stop 2, 8:27			stop 2, 12 minutes
    stop 3, 8:29			stop 3, 14 minutes
    ....					....

Every trip has a route story, and a start time:

    trip1:   stop 1, 8:15; stop 2, 8:27; stop 3, 8:29 ....
    trip2:   stop 1, 8:35; stop 2, 8:47; stop 3, 8:49 ....

becomes:
    route story 1:	stop 1, 0; stop 2, 12 minutes; stop 3, 14 minutes ...
    trip1: route_story 1, start_time  8:15
    trip2: route_story 2, start_time  8:35

Why is this good?
    - it saves a lot of space, and saves loading time if you need to analyse all the trips.
    - it makes logical sense, and makes it easy to examine the changes in route stories per route.

"""

import csv
import os
import sys
from collections import defaultdict, namedtuple
import argparse
# from configparser import ConfigParser

from gtfs.parser.gtfs_reader import StopTime
import logging
import mmh3

import psycopg2
import psycopg2.extras


def parse_config(config_file_name):
    with open(config_file_name) as f:
        # add section header because config parser requires it
        config_file_content = '[Section]\n' + f.read()
    config = ConfigParser()
    config.read_string(config_file_content)
    return {k: v for (k, v) in config['Section'].items()}


def parse_timestamp(timestamp):
    """Returns second since start of day"""
    # We need to manually parse because there's hours >= 24; but ain't Python doing it beautifully?
    (hour, minute, second) = (int(x) for x in timestamp.split(':'))
    return hour * 60 * 60 + minute * 60 + second


def format_time(seconds_from_midnight):
    return '%02d:%02d:%02d' % (
        seconds_from_midnight / 3600, seconds_from_midnight % 3600 / 60, seconds_from_midnight % 60)


class RouteStoryStop:
    def __init__(self, arrival_offset, departure_offset, stop_id, stop_sequence, drop_off_only,
                 pickup_only, shape_dist_traveled):
        self.arrival_offset = arrival_offset
        self.departure_offset = departure_offset
        self.stop_id = stop_id
        self.stop_sequence = stop_sequence
        self.drop_off_only = drop_off_only
        self.pickup_only = pickup_only
        self.shape_dist_traveled = shape_dist_traveled

    def __hash__(self):
        return hash(self.as_tuple())

    def __eq__(self, other):
        return self.as_tuple() == other.as_tuple()

    def as_tuple(self):
        return self.arrival_offset, self.departure_offset, self.stop_id, self.drop_off_only, self.pickup_only

    def __str__(self):
        return 'stop_id=%s,stop_sequence=%s' % (self.stop_id, self.stop_sequence)

    def __repr__(self):
        return 'stop_id=%s,stop_sequence=%s' % (self.stop_id, self.stop_sequence)

    @classmethod
    def from_csv(cls, csv_record):
        route_story_id = int(csv_record['route_story_id'])
        field_names = ['arrival_offset', 'departure_offset', 'stop_id', 'stop_sequence', 'pickup_type',
                       'drop_off_type', 'shape_dist_traveled']
        fields = [csv_record[field] for field in field_names]
        fields = [int(field) if field != '' else 0 for field in fields]
        return route_story_id, cls(*fields)


class RouteStory:
    def __init__(self, route_story_id, stops):
        self.route_story_id = route_story_id
        self.stops = stops
        self.as_string = "-".join("%s-%s" % (stop.stop_id, stop.arrival_offset) for stop in self.stops)
        self.key = mmh3.hash64(self.as_string)[0]


class RouteStorySummary:
    def __init__(self, route_story_id, as_string, key):
        self.route_story_id = route_story_id
        self.as_string = as_string
        self.key = key


class RouteStoryFinder:
    def __init__(self):
        self.data = defaultdict(list)
        self.size = 0

    def add(self, route_story):
        self.data[route_story.key].append(route_story)
        self.size += 1

    def find(self, route_story):
        for other in self.data[route_story.key]:
            if route_story.as_string == other.as_string:
                return other.route_story_id

    def __iter__(self):
        for l in self.data.values():
            for rs in l:
                yield rs


TripRouteStory = namedtuple('TripRouteStory', 'start_time route_story')


def stop_times_file_generator(stop_times_file):
    """Yields a sequence of (trip_id, StopTime) tuples read from gtfs stop_times.txt file"""

    def key(line):
        tokens = line.strip().split(',')
        return tokens[0], int(tokens[4])

    def line_to_trip_and_stop_time(line):
        return line.partition(',')[0], StopTime.from_line(line)

    with open(stop_times_file, encoding='utf8') as f:
        next(f)
        lines = sorted(f.readlines(), key=key)
        return (line_to_trip_and_stop_time(line) for line in lines)


def connect_to_db(config):
    template = "dbname={d[db_name]} user={d[db_user]} host={d[db_host]} password={d[db_password]}"
    connection_str = template.format(d=config)
    logging.debug("Connection to db")
    return psycopg2.connect(connection_str)


def stop_times_db_generator(connection, db_table='gtfs_stop_times'):
    def total_records():
        c = connection.cursor()
        c.execute("SELECT COUNT(*) FROM %s;" % db_table)
        return c.fetchone()[0]

    def progress(iterable):
        for i, value in enumerate(iterable):
            if i % 1000000 == 0 and i > 0:
                logging.debug("%dM records read (%.1f%%)" % (i / 1000000, 100 * i / total_records))
            yield value

    """Yields a sequence of (trip_id, StopTime) tuples read from db"""
    logging.debug("Fetching total number of records")
    total_records = total_records()
    logging.debug("There are %d records in stop_times table" % total_records)
    logging.debug("Creating cursor")
    cursor = connection.cursor('read_stop_times_from_db', cursor_factory=psycopg2.extras.NamedTupleCursor)
    logging.debug("Executing select query")
    cursor.execute("SELECT trip_id,arrival_time,departure_time,stop_id,stop_sequence,drop_off_only,"
                   "pickup_only,shape_dist_traveled" +
                   " FROM %s ORDER BY trip_id, stop_sequence;" % db_table)
    logging.debug("Starting iteration")
    for r in progress(cursor):
        yield r.trip_id, StopTime(parse_timestamp(r.arrival_time),
                                  parse_timestamp(r.departure_time), r.stop_id, r.stop_sequence,
                                  r.drop_off_only, r.pickup_only, r.shape_dist_traveled)
    logging.debug("Done iteration. Closing db connection.")


def group_by_trip_id(sequence):
    """
    Receives a sequence of (trip_id, stop_time) tuples, where stop_time is a StopTime objects. Yields
    (trip_id, stop_times), where stop_times is a list of StopTime objects.
    """
    trips_count = 0
    bad_trips_count = 0

    trip_stop_times = []
    prev_trip_id = None
    for trip_id, stop_time in sequence:
        if prev_trip_id is not None and trip_id != prev_trip_id:
            trips_count += 1
            if trip_stop_times[0].stop_sequence == 1 and trip_stop_times[-1].stop_sequence == len(trip_stop_times):
                yield prev_trip_id, trip_stop_times
            else:
                logging.error("Bad sequence for trip %s: %s" % (prev_trip_id, trip_stop_times))
                bad_trips_count += 1
            trip_stop_times = []
        prev_trip_id = trip_id
        trip_stop_times.append(stop_time)

    if len(trip_stop_times) > 0:
        trips_count += 1
        yield prev_trip_id, trip_stop_times

    logging.debug("Total number of trips in raw data: %d" % trips_count)
    logging.debug("Number of bad trips: %d" % bad_trips_count)


def build_route_stories(trip_and_stop_times):
    """ Builds route stories. Returns a dictionary from id to RouteStory object, and a dictionary
    from trip to a tuple, (route_story_id, start_time)"""
    logging.info("Building route stories")
    route_story_finder = RouteStoryFinder()
    trip_to_route_story = {}  # Dict[int, Tuple[int, datetime]]
    for trip_id, stop_times in trip_and_stop_times:
        # get the start time in seconds since the start of the day
        start_time = stop_times[0].arrival_time
        route_story = RouteStory(route_story_id=-1,
                                 stops=[RouteStoryStop(stop_time.arrival_time - start_time,
                                                       stop_time.departure_time - start_time,
                                                       stop_time.stop_id,
                                                       stop_time.stop_sequence,
                                                       stop_time.pickup_type,
                                                       stop_time.drop_off_type,
                                                       stop_time.shape_dist_traveled) for stop_time in stop_times])
        # is it a new route story? if yes, allocate an id
        route_story_id = route_story_finder.find(route_story)
        if not route_story_id:
            route_story.route_story_id = route_story_id = route_story_finder.size + 1
            route_story_finder.add(route_story)
        trip_to_route_story[trip_id] = (route_story_id, start_time)

    logging.info("%d route stories built" % len(route_story_finder.data))
    return route_story_finder, trip_to_route_story


def export_route_story_stops_to_csv(output_file, route_stories):
    logging.info("Exporting route story stops")
    with open(output_file, 'w') as f:
        f.write(
            "route_story_id,arrival_offset,departure_offset,stop_id,stop_sequence,pickup_type,drop_off_type,shape_dist_taveled\n")
        for route_story in route_stories:
            for i, stop in enumerate(route_story.stops):
                values = [route_story.route_story_id,
                          stop.arrival_offset,
                          stop.departure_offset,
                          stop.stop_id,
                          i + 1,
                          stop.drop_off_only,
                          stop.pickup_only,
                          stop.shape_dist_traveled]
                f.write(','.join(str(x) if x is not None else '' for x in values) + '\n')
    logging.info("Route story stops export done")


def export_trip_route_stories_to_csv(output_file, trip_to_route_story):
    logging.info("exporting %d full trips" % len(trip_to_route_story))
    with open(output_file, 'w') as f2:
        fields = ["trip_id", "route_story", "start_time"]
        writer = csv.DictWriter(f2, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        for trip_id, (route_story_id, start_time) in trip_to_route_story.items():
            writer.writerow({"trip_id": trip_id,
                             "start_time": format_time(start_time),
                             "route_story": route_story_id})
    logging.info("Trips export done.")


def export_route_story_summaries_to_csv(output_file, route_stories):
    logging.info("Exporting route story summaries")
    with open(output_file, 'w', encoding='utf8') as f:
        f.write('route_story_id,rs_hash,rs_string\n')
        for route_story in route_stories:
            f.write("%s,%s,%s\n" % (route_story.route_story_id, route_story.key, route_story.as_string))
    logging.info("Route story summaries export done")


def load_route_stories_from_csv(route_stories_file, trip_to_route_story_file):
    """Reads route stories as written by export_route_stories_to_csv, export_trip_route_stories_to_csv.

    Returns a tuple (route_stories,  trip_to_route_story):
    route_stories: a dictionary from route_story_id to route_story object
    trip_to_route_story: a dictionary from trip_id to a TripRouteStory named tuple
    """
    route_story_id_to_stops = defaultdict(lambda: [])
    with open(route_stories_file, encoding='utf8') as f:
        for record in csv.DictReader(f):
            trip_story_id, trip_story_stop = RouteStoryStop.from_csv(record)
            route_story_id_to_stops[trip_story_id].append(trip_story_stop)

    route_stories = {route_story_id: RouteStory(route_story_id, stops) for route_story_id, stops in
                     route_story_id_to_stops.items()}

    trip_to_route_story = {}
    with open(trip_to_route_story_file, encoding='utf8') as f:
        for record in csv.DictReader(f):
            trip_id = record['trip_id']
            start_time = parse_timestamp(record['start_time'])
            route_story = int(record['route_story'])
            trip_to_route_story[trip_id] = TripRouteStory(start_time, route_stories[route_story])
    return route_stories, trip_to_route_story


# def main():
#     logging.basicConfig(level=logging.DEBUG,
#                         format='%(asctime)s %(message)s',
#                         handlers=[logging.StreamHandler(sys.stdout)])
#     config = parse_config(sys.argv[1])
#     connection = None
#     if config["source"] == "file":
#         logging.info("Loading data from file", config["source"])
#         source_file_name = config["source_file_name"]
#         source_data = stop_times_file_generator(source_file_name)
#     elif config["source"] == "db":
#         logging.info("Loading data from db")
#         connection = connect_to_db(config)
#         source_data = stop_times_db_generator(connection, db_table=config["db_table"])
#     else:
#         raise Exception("Unknown source type %s" % config["source"])
#
#     stories, trips = build_route_stories(group_by_trip_id(source_data))
#
#     if connection:
#         connection.close()
#
#     output_folder = config["output_folder"]
#     export_route_story_stops_to_csv(os.path.join(output_folder, 'route_story_stops.txt'), stories)
#     export_trip_route_stories_to_csv(os.path.join(output_folder, 'trip_route_stories.txt'), trips)
#     export_route_story_summaries_to_csv(os.path.join(output_folder, 'route_story_summaries.txt'), stories)


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])

    parser = argparse.ArgumentParser(description='Create route stories.')
    parser.add_argument('--db_table', help='table to read stop_times from')
    parser.add_argument('--db_name', help='database to read from (default: obus)', default='obus')
    parser.add_argument('--db_user', help='database user name (default: obus)', default='obus')
    parser.add_argument('--db_host', help='database host (default: localhost)', default='localhost')
    parser.add_argument('--db_password', help='database password')
    parser.add_argument('--output_folder', help='where to write the result files')

    args = parser.parse_args()
    config = {
        'db_name': args.db_name,
        'db_user': args.db_user,
        'db_host': args.db_host,
        'db_password': args.db_password
    }

    with connect_to_db(config) as connection:
        source_data = stop_times_db_generator(connection, db_table=args.db_table)
        stories, trips = build_route_stories(group_by_trip_id(source_data))

    output_folder = args.output_folder
    export_route_story_stops_to_csv(os.path.join(output_folder, 'route_story_stops.txt'), stories)
    export_trip_route_stories_to_csv(os.path.join(output_folder, 'trip_route_stories.txt'), trips)
    export_route_story_summaries_to_csv(os.path.join(output_folder, 'route_story_summaries.txt'), stories)


if __name__ == '__main__':
    main()
