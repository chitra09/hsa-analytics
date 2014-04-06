
--define VAR datafu.pig.stats.VAR();
REGISTER './utils.py' using jython as utils


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


transactions = filter transactions by ((category == 'ContEmployee') OR (category == 'ContEmployer')) AND (paymentAvailableDate matches '.*2013.*');

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


trasactions_dates_in_number = foreach transactions generate memberId, amount, (int)REGEX_EXTRACT(paymentAvailableDate, '(.*)-(.*)-(.*)', 1) as year, (int)REGEX_EXTRACT(paymentAvailableDate, '(.*)-(.*)-(.*)', 2) as month , (int)REGEX_EXTRACT(paymentAvailableDate, '(.*)-(.*)-(.*) (.*)', 3) as day, CONCAT('0','') as numberOfDaysToNextDeposit;

trasactions_dates_in_number = filter trasactions_dates_in_number by year == 2013;


trasactions_dates_in_number  = order trasactions_dates_in_number by memberId, year, month, day;

trasactions_dates_in_number = group trasactions_dates_in_number by memberId;

--trasactions_dates_in_number: {group: int,trasactions_dates_in_number: {(memberId: int,amount: double,year: int,month: int,day: int, numberOfDaysToNextDeposit:int)}}
describe trasactions_dates_in_number;



describe trasactions_dates_in_number;

trasactions_dates_in_number = foreach trasactions_dates_in_number {
	
	orderBag 		= order trasactions_dates_in_number by year, month, day;
	orderBag 		= utils.calculateNumberOfDaysTilNextDeposit(orderBag);
	--standarDev 	= utils.calculateSD(orderBag, averagePerBag);
	generate  flatten(group) , flatten(orderBag);
}

--{group: int,output::memberId: int,output::amount: double,output::year: int,output::month: int,output::day: int,output::numberOfDaysToNextDeposit: int,output::average: double}

trasactions_dates_in_number = foreach trasactions_dates_in_number generate output::memberId as memberId, output::amount as amount, output::year as year, output::month as month, output::day as day,  output::numberOfDaysToNextDeposit as numberOfDaysToNextDeposit,   output::average as average, (numberOfDaysToNextDeposit - average) as distance ;


trasactions_dates_in_number = group trasactions_dates_in_number by memberId;

trasactions_dates_in_number = foreach trasactions_dates_in_number {
	

	stdDev = utils.calculateStandarDev(trasactions_dates_in_number);
	generate flatten(group), flatten(trasactions_dates_in_number), stdDev;


}

--trasactions_dates_in_number: {memberId: int,amount: double,year: int,month: int,day: int,numberOfDaysToNextDeposit: int,average: int,distance: int}



trasactions_dates_in_number = join trasactions_dates_in_number by memberId, member_dependents by memberId;


trasactions_dates_in_number = foreach trasactions_dates_in_number generate member_dependents::members_age::memberId as memberId, ageMember, gender, state, duration, numOfDependents, amount, numberOfDaysToNextDeposit, average, trasactions_dates_in_number::standarDev as stdDev;


trasactions_dates_in_number = foreach trasactions_dates_in_number {
	

	deposit = utils.validateDeposit(stdDev,numberOfDaysToNextDeposit);
	generate memberId,  ageMember, gender, state, duration, numOfDependents, amount, numberOfDaysToNextDeposit, average, stdDev, deposit;

}



-- output::numberOfDaysToNextDeposit

dump 		trasactions_dates_in_number;
describe 	trasactions_dates_in_number;
--describe 	trasactions_dates_in_number;


STORE trasactions_dates_in_number INTO 'output' USING org.apache.pig.piggybank.storage.MultiStorage('trasactions_dates_in_number', '0', 'none', ',');


--dump trasactions_dates_in_number;



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


