REGISTER './utils.py' using jython as utils
REGISTER '$hbase';
--REGISTER '/Users/Miguel/Documents/Servers/hbase-0.94.17/hbase-0.94.17.jar';
--REGISTER '/Users/Miguel/Documents/Servers/hbase-0.94.17/lib/protobuf-java-2.4.0a.jar';

/*
***********************************************************
 Read the data from HBASE
***********************************************************
*/

members = load '$member' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('M:state, M:zip, M:g, M:birthYr, M:effectiveDt, D:dID, D:dRel, D:dBirthYr, D:g, D:dState, D:dZip', '-loadKey true') as (key:chararray, state:chararray, zip:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray,  dependentId:chararray, relationship:chararray, dependentBirthYear:int, dependentGender:chararray, dependentState:chararray, dependentZip:chararray);


members = foreach members generate utils.parseMembers(key) as keyMap, state, zip, gender, birthYear, hsaEffectiveDate, dependentId, relationship, dependentBirthYear, dependentGender, dependentState, dependentZip;
members = foreach members generate  keyMap#'memberId' as memberId, state, zip, gender, birthYear, hsaEffectiveDate, dependentId, relationship, dependentBirthYear, dependentGender, dependentState, dependentZip;


transactions = load '$transactions' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('D:amt', '-loadKey true') as (key:chararray, amount:double);
transactions = foreach transactions generate utils.parseTransactions(key) as keyMap, amount;
transactions = foreach transactions generate keyMap#'memberId' as memberId, keyMap#'category' as category, keyMap#'paymentDate' as paymentDate, amount; 


claims = load '$claims' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('D:cpt, D:dependent, D:type, D:dtRcvd, D:start, D:end, D:repricedAmount, D:amt', '-loadKey true') as (key:chararray, cpt:chararray, dependentService:chararray, claimType:chararray, dateReceived:chararray, serviceStartDate:chararray, serviceEndDate:chararray, repricedAmount:double, patientAmount:double);
claims = foreach claims generate utils.parseClaims(key) as keyMap, cpt, dependentService, claimType, dateReceived, serviceStartDate, serviceEndDate, repricedAmount, patientAmount;
claims = foreach claims generate keyMap#'memberId' as memberId, keyMap#'claimId' as claimId, keyMap#'dateProcessed' as dateProcessed, cpt, CONCAT('0',dependentService) as dependentService, claimType, dateReceived, serviceStartDate, serviceEndDate, repricedAmount, patientAmount;

/*
***********************************************************
 Data transformation
***********************************************************
*/


members_age = foreach members generate memberId,  (GetYear(CurrentTime()) - birthYear) as ageMember, gender, dependentId, relationship, (GetYear(CurrentTime()) - dependentBirthYear) as ageDependent, dependentGender, state;

claims_dates =  foreach claims generate claimId, memberId, dependentService, claimType, (chararray) REGEX_EXTRACT(serviceStartDate, '(.*)-(.*)', 1) as serviceStartDate, repricedAmount, patientAmount,cpt;

/*
***********************************************************
 Joins, Groups, Arithmetic operations and Store
***********************************************************
*/


--Transformation 1:  Monthly doctor visits for a member, dependent(based on claims)

member_claims = group claims_dates by (memberId, dependentService, serviceStartDate);
monthly_member_visits = foreach member_claims generate flatten(group), COUNT(claims_dates) as visits;

--Output format: MemberID, DependentID, Month_Of_Service, Count
store monthly_member_visits into '$monthly_member_visits' using PigStorage(',');



--Transformation 2: Monthly doctor visits based on age of member/dependent and claims

claims_dates_members_dependents = join members_age by (memberId, dependentId), claims_dates by (memberId, dependentService);
age_claims = group claims_dates_members_dependents by (members_age::ageDependent, claims_dates::serviceStartDate);
monthly_visits_by_age = foreach age_claims generate flatten(group), COUNT(claims_dates_members_dependents) as visits;

--Output format: Age, Month_Of_Service, Count
store monthly_visits_by_age into '$monthly_visits_by_age' using PigStorage(',');



--Transformation 3: Monthly members doctor visits vs HSA Account balance

transactions_members = join members by memberId, transactions by memberId;
transactions_dates = foreach transactions_members generate transactions::memberId as memberId, amount, (chararray)REGEX_EXTRACT(paymentDate, '(.*)-(.*)', 1) as paymentDate;


member_balances = group transactions_dates by (memberId, paymentDate);

monthly_member_balance = foreach member_balances generate flatten(group),(double) SUM(transactions_dates.amount) as amount;

member_payments_claims  = group claims_dates by (memberId, serviceStartDate);

monthly_member_payments_claims = foreach member_payments_claims generate flatten(group) , (double) SUM(claims_dates.patientAmount) as patientAmount ;

monthly_member_balance_monthly_member_payments_claims = join monthly_member_balance by (group::memberId, group::paymentDate), monthly_member_payments_claims by (group::memberId, group::serviceStartDate);

monthly_member_balance_spends = foreach monthly_member_balance_monthly_member_payments_claims generate monthly_member_balance::group::memberId as memberId, monthly_member_balance::group::paymentDate as monthOfService, monthly_member_balance::amount as accountBalance, monthly_member_payments_claims::patientAmount as amountSpent;

--Output format: MemberID, Month_Of_Service, Account_Balance, Amount_Spent
store monthly_member_balance_spends into '$monthly_member_balance_spends' using PigStorage(',');




--Transformation 4: Top claim types based on member/dependent age

members_age_claims_dates = join members_age by (memberId,dependentId), claims_dates by (memberId,dependentService);

members_age_cpt_claims 	=  	group members_age_claims_dates by (ageDependent, cpt);
claims_types_by_age 	= 	foreach members_age_cpt_claims generate flatten(group), COUNT(members_age_claims_dates) as cptCases;


claims_types_by_age_grouped = group claims_types_by_age by (ageDependent);

top_claims_types_by_age  = foreach claims_types_by_age_grouped {
	result = TOP(10, 2, claims_types_by_age);
	GENERATE FLATTEN(result);
}

--output format:age, ctpCpde, number_patients
store top_claims_types_by_age into '$top_claims_types_by_age' using PigStorage(',');



--Transformation 5: Yearly report on members who are maxing out on their contributions


transactions_year_dates = foreach transactions generate memberId, (int)REGEX_EXTRACT(paymentDate, '(.*)-(.*)-(.*)', 1) as year, amount;

transactions_year_dates = filter transactions_year_dates by year >0 ;

transactions_year_dates = group transactions_year_dates by (memberId,year);

transactions_year_dates = foreach transactions_year_dates generate flatten(group) , (double)SUM(transactions_year_dates.amount) as amount, CONCAT('F','') as isMaxed;



transactions_year_dates = order transactions_year_dates by memberId, year asc;

transactions_year_dates = group transactions_year_dates by memberId;


transactions_year_dates = foreach transactions_year_dates {
	
	orderBag =  order transactions_year_dates by group::year;
 	result = utils.reviewMaxMin(group,orderBag);
 	generate  flatten(group) , flatten(result);
}


transactions_year_dates = foreach transactions_year_dates generate output::group::memberId as memberId, output::group::year as year, output::amount as amount, output::isMaxed as isMaxed;

transactions_year_dates = join members_age by memberId ,transactions_year_dates by memberId;

transactions_year_dates = foreach transactions_year_dates generate transactions_year_dates::memberId as memberId, members_age::ageMember as age, members_age::state as state, transactions_year_dates::amount as contribution, transactions_year_dates::year as year, transactions_year_dates::isMaxed as isMaxed;

transactions_year_dates = distinct transactions_year_dates;

transactions_year_dates = order transactions_year_dates by memberId, year asc; 

describe transactions_year_dates;

--output format:memberId, year, amount, maxed
store transactions_year_dates into '$yearly_report_maxing_contributions' using PigStorage(',');




