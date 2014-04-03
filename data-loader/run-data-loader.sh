#!/bin/bash


hadoop fs -rmr /HSAMembersJoined
hadoop fs -rmr /HSAClaimsJoined
hadoop fs -rmr /MemberLoad
hadoop fs -rmr /ClaimLoad
hadoop fs -rmr /TransactionLoad

echo =======================================================
echo "Joining Members data "
pig src/main/scripts/join-members.pig
wait


echo =======================================================
echo "Joining Claims data "
pig src/main/scripts/join-claims.pig
wait


echo "Starting the Data Loader..."
hadoop jar target/data-loader-1.0-SNAPSHOT.jar com.thinkbiganalytics.hsa.analytics.DataLoader loader.properties /HSAMembersJoined /MemberLoad /HSAClaimsJoined /ClaimLoad /HSATransaction /TransactionLoad

echo =======================================================
exit 0

