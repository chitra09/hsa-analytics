
/*
***********************************************************
 Read the data from HDFS
***********************************************************
*/

claims = load '$inputClaim' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:double );

claims_details = load '$inputClaimDetails' using PigStorage(',') as (claimId:int, cptCode:chararray); 

members = load '$inputMember' using PigStorage(',') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);

dependents = load '$inputDependent' using PigStorage(',') as (memberId:int, dependentId:int, relationship:chararray, birthYear:int, gender:chararray, state:chararray, zip:chararray); 

transactions = load '$inputTransactions' using PigStorage(',') as (memberId:int, amount:double, category:chararray, paymentAvailableDate:chararray);