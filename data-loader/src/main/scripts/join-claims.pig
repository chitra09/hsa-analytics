
/*
*********************************************************************
 Read the Claims data and Claims Details from HDFS and Join the data
*********************************************************************
*/

claims = load '/HSAClaim/Claim.csv' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientAmount:double );

claims_details = load '/HSAClaim/ClaimDetail.csv' using PigStorage(',') as (claimId:int, cptCode:chararray); 

joined = join claims by claimId, claims_details by claimId;
claims_output = foreach joined generate claims::claimId, memberId, dependentService, claimType, dateReceived, dateProcessed, serviceStart, serviceEnd,repricedAmount, patientAmount, cptCode;

store claims_output into '/HSAClaimsJoined' using PigStorage(',');


