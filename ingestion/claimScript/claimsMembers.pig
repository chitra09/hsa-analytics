                                

/*
***********************************************************
 Read the data
***********************************************************
*/
                                                                                              
claims_s = load '$inputClaim' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:chararray );

claims_detail_s = load '$inputClaimDetails' using PigStorage(',') as (claimId:int, cptCode:chararray); 

member_s = load '$inputMember' using PigStorage(',') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);

dependent_s = load '$inputDependent' using PigStorage(',') as (memberId:int, dependentId:int, relationship:chararray, birthYear:int, gender:chararray, state:chararray, zip:chararray); 

/*
***********************************************************
 Data transformation
***********************************************************
*/

members_age 	= foreach member_s generate memberId,  (GetYear(CurrentTime()) - birthYear) as ageMember, gender;

dependents_age 	= foreach dependent_s generate  dependentId, memberId, relationship, (GetYear(CurrentTime()) - birthYear) as ageDependent, gender;

claims_dates =  foreach claims_s generate claimId, memberId, dependentService, claimType,  REGEX_EXTRACT(serviceStart, '(.*)-(.*)', 1) as serviceStartDate, repricedAmount, patientResponsabilityAmount;



/*
***********************************************************
 Data joins
***********************************************************
*/

join_claims_claims_details = join claims_s by claimId, claims_detail_s by claimId;

join_claims_dates_claims_details = join claims_dates by claimId, claims_detail_s by claimId;

join_claims_members_age = join join_claims_claims_details by memberId, members_age by memberId;

join_members_dependents = join members_age by memberId, dependents_age by memberId;

join_clamis_dates_members_dependents = join join_members_dependents by members_age::memberId, join_claims_dates_claims_details by memberId;


/*
***********************************************************
 Group 
***********************************************************
*/

group_claims_members = group join_clamis_dates_members_dependents by (join_members_dependents::members_age::memberId , join_claims_dates_claims_details::claims_dates::serviceStartDate);

/*
***********************************************************
 Aritmetic operations 
***********************************************************
*/

                                                             
monthy_member_visits = foreach group_claims_members generate group, 		COUNT(join_clamis_dates_members_dependents) as visits ; 

/*

***********************************************************
 Data cleaning
***********************************************************
*/

claims_detail = foreach join_claims_members_age generate claims_s::claimId as claimId, claims_detail_s::cptCode as cptCode,  claims_s::memberId as memberId, members_age::ageMember as memberAge, claims_s::dateProcessed as dateProcessed, claims_s::patientResponsabilityAmount as patientResponsabilityAmount, claims_s::repricedAmount as repricedAmount; 

/*
***********************************************************
 Data describes
***********************************************************
*/
describe join_clamis_dates_members_dependents;
describe claims_s;
describe member_s;
describe members_age;
describe claims_detail_s;
describe join_claims_claims_details;
describe join_claims_members_age;
describe claims_detail;

describe group_claims_members;


describe monthy_member_visits;

dump monthy_member_visits;
