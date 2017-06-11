#!/usr/bin/env bash


if [ "$#" -ne 3 ]; then
    echo "Usage: insert_gtfs.sh  gtfs_file db_username db_password"
    exit -1
fi

# allow running psql w/o supplying password
export PGPASSWORD=$3
username=$2

# unzip the gtfs
mkdir -p /tmp/gtfs
time unzip -o -d /tmp/gtfs $1

# load stop times, needed for creating route stories
time psql -h 127.0.0.1  -U $2 obus < incremental_load_tmp_stop_times.sql


# run route story creation
python3 -m gtfs.parser.route_stories route_stories_config db_user=${username} db_password=$3 output_folder=/tmp/gtfs db_table=tmp_stop_times

# run file import to tmp tables
time psql -h 127.0.0.1  -U ${username} obus < incremental_load_tmp_tables.sql

# calculate the variables the script is going to need
gtfs_date=`psql -h 127.0.0.1 -U postgres obus -t -c "select min(start_date) from tmp_calendar"`
file_size=`wc -c $1  | cut -f1 -d" "`

# run the insert!
time psql -h 127.0.0.1 -U ${username} -v gtfs_date="${gtfs_date}" -v file_size=${file_size} obus  < incremental_insert.sql

# run the cleanup
time psql -h 127.0.0.1 -U ${username} obus  < incremental_cleanup.sql

