
define VAR datafu.pig.stats.VAR();

/*
***********************************************************
 Read the data from HDFS
***********************************************************
*/

claims = load '$inputClaim' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:double );

claims_details = load '$inputClaimDetails' using PigStorage(',') as (claimId:int, cptCode:chararray); 

members = load '$inputMember' using PigStorage(',') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);
members = foreach members generate memberId, state, cp, gender, birthYear, ToDate(hsaEffectiveDate,'yyyy-mm-dd HH:mm:ss.SSS') as hsaEffectiveDate;

dependents = load '$inputDependent' using PigStorage(',') as (memberId:int, dependentId:int, relationship:chararray, birthYear:int, gender:chararray, state:chararray, zip:chararray); 

transactions = load '$inputTransactions' using PigStorage(',') as (memberId:int, amount:double, category:chararray, paymentAvailableDate:chararray);

/*
***********************************************************
 Data transformation
***********************************************************
*/

members_age 	= foreach members generate memberId,  (GetYear(CurrentTime()) - birthYear) as ageMember, gender, state, MonthsBetween(CurrentTime(), hsaEffectiveDate) as duration;


dependentGroup = group dependents by memberId;
dependentCount = foreach dependentGroup generate flatten(group) as memberId, COUNT(dependents) as numOfDependents:long;
dependentCount = foreach dependentCount generate memberId, (numOfDependents - 1) as numOfDependents;

members_dependents = join members_age by memberId, dependentCount by memberId;
member_dependents = foreach members_dependents generate members_age::memberId, ageMember, gender, state, duration, numOfDependents;

--member_transactions = join member_dependents by memberId, transactions by memberId;
--member_deposits = foreach member_deposits generate member_dependents::memberId, ageMember, gender, state, duration, numOfDependents, amount, paymentAvailableDate;

member_deposits = filter transactions by ((category == 'ContEmployee') OR (category == 'ContEmployer')) AND (paymentAvailableDate matches '.*2013.*');
member_deposits = foreach member_deposits generate memberId, paymentAvailableDate;

member_deposits = group member_deposits by memberId;
member_deposits_var = foreach member_deposits generate flatten(group) as memberId, member_deposits as datesBag;
dump member_deposits_var;


deposits_timeline = foreach member_deposits generate SUM(datesBag.paymentAvailableDate) as sum;


claims_dates =  foreach claims generate claimId, memberId, dependentService, claimType, (chararray) REGEX_EXTRACT(serviceStart, '(.*)-(.*)', 1) as serviceStartDate, repricedAmount, (double)patientResponsabilityAmount;


trasactions_dates = foreach transactions generate memberId, amount, (chararray)REGEX_EXTRACT(paymentAvailableDate, '(.*)-(.*)', 1) as paymentDate;


/*
***********************************************************
 Data joins
***********************************************************
*/

--join_claims_claims_details = join claims_s by claimId, claims_detail_s by claimId;

--join_claims_members_age = join join_claims_claims_details by memberId, members_age by memberId;

claims_dates_claims_details = join claims_dates by claimId, claims_details by claimId;


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
                                                             
monthly_member_visits = foreach member_claims generate flatten(group), COUNT(claims_dates) as visits;
monthly_visits_by_age = foreach age_claims generate flatten(group), COUNT(claims_dates_members_dependents) as visits;
monthly_member_balance = foreach member_balances generate flatten(group) ,(double) SUM(trasactions_dates.amount) as amount;

monthly_member_payments_claims = foreach member_payments_claims generate flatten(group) , (double) SUM(claims_dates.patientResponsabilityAmount) as patientResponsabilityAmount ;

montly_member_balance_monthly_member_payments_claims = join monthly_member_balance by (group::memberId, group::paymentDate), monthly_member_payments_claims by (group::memberId, group::serviceStartDate);


montly_member_balance_spends = foreach montly_member_balance_monthly_member_payments_claims generate monthly_member_balance::group::memberId as memberId, monthly_member_balance::group::paymentDate as monthOfService, monthly_member_balance::amount as accountBalance, monthly_member_payments_claims::patientResponsabilityAmount as amountSpent;


/*
***********************************************************
 Data describes
***********************************************************


describe claims_dates_members_dependents;
describe claims;
describe members;
describe members_age;
describe claims_details;
describe member_claims;
describe monthly_member_visits;
describe monthly_visits_by_age;
describe monthly_member_balance;
describe monthly_member_payments_claims;
describe montly_member_balance_monthly_member_payments_claims;
describe montly_member_balance_spends;
*/


/*
***********************************************************
 Store the output
***********************************************************
*/

--Output format: MemberID, DependentID, Month_Of_Service, Count
--store monthly_member_visits into '$monthly_member_visits' using PigStorage(',');

--Output format: Age, Month_Of_Service, Count
--store monthly_visits_by_age into '$monthly_visits_by_age' using PigStorage(',');


--Output format: MemberID, Month_Of_Service, Account_Balance, Amount_Spent
--store montly_member_balance_spends into '$monthly_member_balance_spends' using PigStorage(',');


