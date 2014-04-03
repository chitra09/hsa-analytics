
/*
*********************************************************************
 Read the Members data and Dependents from HDFS and Join the data
*********************************************************************
*/

members = load '/HSAMember/Member.csv' using PigStorage(',') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);

dependents = load '/HSAMember/Dependent.csv' using PigStorage(',') as (memberId:int, dependentId:chararray, relationship:chararray, birthYear:int, gender:chararray, state:chararray, zip:chararray);


joined = join members by memberId, dependents by memberId;

members_output = foreach joined generate members::memberId as memberId, members::state as memberState, members::cp as memberZip, members::gender as memberGender, members::birthYear as memberBirthYear, members::hsaEffectiveDate as memberHasEffectiveDate , dependents::dependentId as dependentId, dependents::relationship as dependentRelationship, dependents::birthYear as dependentBirthYear,  dependents::gender as dependentGender, dependents::state as dependentState, dependents::zip as dependentZip; 

store members_output into '/HSAMembersJoined' using PigStorage(',');
