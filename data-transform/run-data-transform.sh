#!/bin/bash

# source the properties:
. data-transform.properties

if [ "$delete_output" == "true" ]; then
	echo =======================================================
	hadoop fs -rmr $monthly_member_visits
	hadoop fs -rmr $monthly_visits_by_age
	hadoop fs -rmr $monthly_member_balance_spends
	hadoop fs -rmr $top_claims_types_by_age
	hadoop fs -rmr $yearly_report_maxing_contributions

	echo =======================================================
fi

echo =======================================================
echo "Transforming data 2"
pig -m ./data-transform.properties data-transform-hbase.pig
# wait for the pig script to finish
wait
echo =======================================================
exit 0

