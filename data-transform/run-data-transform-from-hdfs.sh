#!/bin/bash

# source the properties:
. data-transform.properties

if [ "$delete_output" == "true" ]; then
	echo =======================================================
	hadoop fs -rmr $monthly_member_visits
	hadoop fs -rmr $monthly_visits_by_age
	hadoop fs -rmr $monthly_member_balance_spends
	echo =======================================================
fi

echo =======================================================
echo "Transforming data "
pig -m ./data-transform.properties data-transform.pig
# wait for the pig script to finish
wait
echo =======================================================
exit 0

