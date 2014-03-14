# A simple utility to get the data into HDFS

# Source the properties
. ingestion.properties

echo "Starting ingest ..."
echo "==========================================================="

now=$(date +"%Y%m%d_%H%M%S")
hadoop fs -mkdir /HSA

# Member details
hadoop fs -mkdir /HSA/Member
hadoop fs -put $member /HSA/Member/Member_$now

# Claims
hadoop fs -mkdir /HSA/Claim
hadoop fs -put $claim /HSA/Claim/Claim_$now

# Claim details
hadoop fs -mkdir /HSA/Claim_Details
hadoop fs -put $claim_details /HSA/Claim_Details/ClaimDetails_$now

# Dependent details
hadoop fs -mkdir /HSA/Dependent
hadoop fs -put $dependent /HSA/Dependent/Dependent_$now

# Transaction details
hadoop fs -mkdir /HSA/Transactions
hadoop fs -put $transactions /HSA/Transactions/Transactions_$now

echo "Data ingested into HDFS"
echo "==========================================================="
