--import 'data-load-hbase.pig';

REGISTER '$hbase';

/*
***********************************************************
 Read the data from HBASE
***********************************************************
*/

members = load '$member' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('M:state, M:zip, M:g, M:birthYr, M:effectiveDt, M:amt, M:category, M:pymtDt, D:dID, D:dRel, D:dBirthYr, D:g, D:dState, D:dZip', '-loadKey true') as (memberId:chararray, state:chararray, zip:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray, amount:double, category:chararray, paymentDate:chararray, dependentId:chararray, relationship:chararray, dependentBirthYear:int, dependentGender:chararray, dependentState:chararray, dependentZip:chararray);


claims = load '$claims' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('D:mID, D:dependent, D:type, D:dtRcvd, D:dtProcessed, D:start, D:end, D:repricedAmount, D:amt', '-loadKey true') as (claimId:chararray, memberId:chararray, dependentService:chararray, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStartDate:chararray, serviceEndDate:chararray, repricedAmount:double, patientAmount:double );


/*
***********************************************************
 Data transformation
***********************************************************
*/

members_age = foreach members generate memberId,  (GetYear(CurrentTime()) - birthYear) as ageMember, gender, dependentId, relationship, (GetYear(CurrentTime()) - birthYear) as ageDependent, dependentGender;

claims_dates =  foreach claims generate claimId, memberId, dependentService, claimType, (chararray) REGEX_EXTRACT(serviceStartDate, '(.*)-(.*)', 1) as serviceStartDate, repricedAmount, patientAmount;

transactions_dates = foreach members generate memberId, amount, (chararray)REGEX_EXTRACT(paymentDate, '(.*)-(.*)', 1) as paymentDate;

/*
***********************************************************
 Joins, Groups, Arithmetic operations and Store
***********************************************************
*/


--Transformation 1:  Monthly doctor visits for a member, dependent(based on claims)

member_claims = group claims_dates by (memberId, dependentService, serviceStartDate);
monthly_member_visits = foreach member_claims generate flatten(group), COUNT(claims_dates) as visits;

--Output format: MemberID, DependentID, Month_Of_Service, Count
--store monthly_member_visits into '$monthly_member_visits' using PigStorage(',');



--Transformation 2: Monthly doctor visits based on age of member/dependent and claims

claims_dates_members_dependents = join members_age by (memberId, dependentId), claims_dates by (memberId, dependentService);
age_claims = group claims_dates_members_dependents by (members_age::ageDependent, claims_dates::serviceStartDate);
monthly_visits_by_age = foreach age_claims generate flatten(group), COUNT(claims_dates_members_dependents) as visits;

--Output format: Age, Month_Of_Service, Count
--store monthly_visits_by_age into '$monthly_visits_by_age' using PigStorage(',');



--Transformation 3: Monthly members doctor visits vs HSA Account balance

member_balances = group transactions_dates by (memberId, paymentDate);

monthly_member_balance = foreach member_balances generate flatten(group),(double) SUM(transactions_dates.amount) as amount;

member_payments_claims  = group claims_dates by (memberId, serviceStartDate);

monthly_member_payments_claims = foreach member_payments_claims generate flatten(group) , (double) SUM(claims_dates.patientAmount) as patientAmount ;

monthly_member_balance_monthly_member_payments_claims = join monthly_member_balance by (group::memberId, group::paymentDate), monthly_member_payments_claims by (group::memberId, group::serviceStartDate);

monthly_member_balance_spends = foreach monthly_member_balance_monthly_member_payments_claims generate monthly_member_balance::group::memberId as memberId, monthly_member_balance::group::paymentDate as monthOfService, monthly_member_balance::amount as accountBalance, monthly_member_payments_claims::patientAmount as amountSpent;

--Output format: MemberID, Month_Of_Service, Account_Balance, Amount_Spent
--store monthly_member_balance_spends into '$monthly_member_balance_spends' using PigStorage(',');





