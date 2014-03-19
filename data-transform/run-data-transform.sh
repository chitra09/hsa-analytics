#!/bin/bash

# source the properties:
. data-transform.properties

if [ "$delete_output" == "true" ]; then
   echo =======================================================
   hadoop fs -rmr $monthly_member_visits
   hadoop fs -rmr $monthly_visits_by_age
   echo =======================================================
fi

/Users/chitraramanathan/Programming/pig-0.12.0/bin/pig -m ./data-transform.properties data-transform.pig

# wait for the pig script to finish
wait
echo =======================================================
exit 0
