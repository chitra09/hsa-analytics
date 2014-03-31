#!/bin/bash


hadoop fs -rmr /HSAClaimsJoined
hadoop fs -rmr /MemberLoad
hadoop fs -rmr /ClaimLoad
hadoop fs -rmr /TransactionLoad

echo =======================================================
echo "Joining Claims data "
pig src/main/scripts/join-claims.pig
wait

echo "Starting the Data Loader..."
hadoop jar target/data-loader-1.0-SNAPSHOT.jar com.thinkbiganalytics.hsa.analytics.DataLoader loader.properties /HSAMember /MemberLoad /HSAClaimsJoined /ClaimLoad /HSATransaction /TransactionLoad

echo =======================================================
exit 0

