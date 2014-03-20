                                

/*
***********************************************************
 Read the data
***********************************************************
*/
                                                                                              
claims = load '$inputClaim' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:double );

claims_details = load '$inputClaimDetails' using PigStorage(',') as (claimId:int, cptCode:chararray); 

members = load '$inputMember' using PigStorage(',') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);

dependents = load '$inputDependent' using PigStorage(',') as (memberId:int, dependentId:int, relationship:chararray, birthYear:int, gender:chararray, state:chararray, zip:chararray); 


transactions = load '$inputTransactions' using PigStorage(',') as (memberId:int, amount:double, category:chararray, paymentAvailableDate:chararray);


/*
***********************************************************
 Data transformation
***********************************************************
*/

members_age 	= foreach members generate memberId,  (GetYear(CurrentTime()) - birthYear) as ageMember, gender;

dependents_age 	= foreach dependents generate  dependentId, memberId, relationship, (GetYear(CurrentTime()) - birthYear) as ageDependent, gender;

claims_dates =  foreach claims generate claimId, memberId, dependentService, claimType,  REGEX_EXTRACT(serviceStart, '(.*)-(.*)', 1) as serviceStartDate, repricedAmount, patientResponsabilityAmount;


trasactions_dates = foreach transactions generate memberId, amount, REGEX_EXTRACT(paymentAvailableDate, '(.*)-(.*)', 1) as paymentDate;


/*
***********************************************************
 Data joins
***********************************************************
*/

--join_claims_claims_details = join claims_s by claimId, claims_detail_s by claimId;

--join_claims_members_age = join join_claims_claims_details by memberId, members_age by memberId;

claims_dates_claims_details = join claims_dates by claimId, claims_details by claimId;

members_dependents = join members_age by memberId, dependents_age by memberId;

claims_dates_members_dependents = join members_dependents by (members_age::memberId, dependents_age::dependentId), claims_dates_claims_details by (memberId,dependentService);


/*
***********************************************************
 Group 
***********************************************************
*/

--claims_members = group claims_dates_members_dependents by (members_dependents::members_age::memberId, members_dependents::dependents_age::dependentId, claims_dates_claims_details::claims_dates::serviceStartDate);

member_payments_claims  = group claims_dates by (memberId, serviceStartDate);
member_claims = group claims_dates by (memberId, dependentService, serviceStartDate);
age_claims = group claims_dates_members_dependents by (members_dependents::dependents_age::ageDependent, claims_dates_claims_details::claims_dates::serviceStartDate);

member_balances = group trasactions_dates by (memberId, paymentDate);



/*
***********************************************************
 Arithmetic operations 
***********************************************************
*/
                                                             
--monthy_member_visits = foreach claims_members generate flatten(group), COUNT(claims_dates_members_dependents) as visits ; 
monthly_member_visits = foreach member_claims generate flatten(group), COUNT(claims_dates) as visits;
monthly_visits_by_age = foreach age_claims generate flatten(group), COUNT(claims_dates_members_dependents) as visits;
monthly_member_balance = foreach member_balances generate flatten(group) , SUM(trasactions_dates.amount);

monthly_member_payments_claims = foreach member_payments_claims generate flatten(group) , (double) SUM(claims_dates.patientResponsabilityAmount) ;

describe monthly_member_payments_claims;

describe monthly_member_balance;

/*

***********************************************************
 Data cleaning
***********************************************************
*/

--claims_detail = foreach join_claims_members_age generate claims_s::claimId as claimId, claims_detail_s::cptCode as cptCode,  claims_s::memberId as memberId, members_age::ageMember as memberAge, claims_s::dateProcessed as dateProcessed, claims_s::patientResponsabilityAmount as patientResponsabilityAmount, claims_s::repricedAmount as repricedAmount; 

/*
***********************************************************
 Data describes
***********************************************************
*/

describe claims_dates_members_dependents;
describe claims;
describe members;
describe members_age;
describe claims_details;
--describe join_claims_claims_details;
--describe join_claims_members_age;
describe member_claims;
describe monthly_member_visits;
describe monthly_visits_by_age;
describe monthly_member_balance;
describe monthly_member_payments_claims;

/*
***********************************************************
 Store the output
***********************************************************
*/

--dump monthy_member_visits;
--Output format: MemberID, DependentID, Month_Of_Service, Count
--store monthly_member_visits into '$monthly_member_visits' using PigStorage(',');

--Output format: Age, Month_Of_Service, Count
--store monthly_visits_by_age into '$monthly_visits_by_age' using PigStorage(',');


