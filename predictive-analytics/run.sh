#!/bin/bash

# source the properties:
. analytics.properties

if [ "$delete_output" == "true" ]; then
	echo =======================================================
	hadoop fs -rmr $classifierTraining
	echo =======================================================
fi

echo =======================================================
pig -m ./analytics.properties analytics.pig
# wait for the pig script to finish
wait
echo =======================================================
exit 0

