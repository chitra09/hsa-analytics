#!/bin/bash

# source the properties:
. sample.properties

if [ "$delete_output" == "true" ]; then
   echo =======================================================
   echo Deleting $coutput
   hadoop dfs -rmr $coutput
   echo =======================================================
fi

pig -m ./sample.properties claimsMembers.pig

# wait for the pig script to finish
wait
echo =======================================================
exit 0
