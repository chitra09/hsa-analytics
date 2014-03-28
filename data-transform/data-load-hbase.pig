register '$probuf_folder/protobuf-java-2.4.0a.jar';

/*
register '/Users/Miguel/Documents/Servers/hbase-0.94.17/hbase-0.94.17.jar';

***********************************************************
 Read the data from HBASE
***********************************************************
*/

members = load 'hbase://MEMBER' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('M:state,M:zip,M:g,M:birthYr,M:effectiveDt', '-loadKey true') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);

/*
claims = load '$inputClaim' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:double );
*/

claims = load 'hbase://CLAIM' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('D:mId,D:dependent, D:type, D:dtRcvd, D:dtProcessed, D:start, D:end, D:repricedAmount, D:amt', '-loadKey true') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:double );

claims_details = load 'hbase://CLAIM_DETAIL' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('D:cpt', '-loadKey true')  as (claimId:int, cptCode:chararray);

dependents = load 'hbase://DEPENDENT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('D:dID,D:dRel,D:dBirthYr,D:g,D:State,D:Zip', '-loadKey true')  as (memberId:int, dependentId:int, relationship:chararray, birthYear:int, gender:chararray, state:chararray, zip:chararray); 

transactions = load 'hbase://TRANSATION' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('M:amt,M:category,M:pymtDt', '-loadKey true')  as (memberId:int, amount:double, category:chararray, paymentAvailableDate:chararray);