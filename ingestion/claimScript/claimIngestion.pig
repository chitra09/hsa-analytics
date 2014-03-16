                                

/*
***********************************************************
 Read the data
***********************************************************
*/
                                                                                              
claims_s = load '/HSA/Claim/Claim_20140316_074711' using PigStorage(',') as (claimId:int, memberId:int, dependentService:int, claimType:chararray, dateReceived:chararray, dateProcessed:chararray, serviceStart:chararray, serviceEnd:chararray,repricedAmount:chararray, patientResponsabilityAmount:chararray); 

member_s = load '/HSA/Member/Member_20140316_074711' using PigStorage(',') as (memberId:int, state:chararray, cp:int, gender:chararray, birthYear:int, hsaEffectiveDate:chararray);

claims_detail_s = load '/HSA/Claim_Details/ClaimDetails_20140316_074711' using PigStorage(',') as (claimId:int, cptCode:chararray); 

/*
***********************************************************
 Data transformation
***********************************************************
*/

member_age = foreach member_s generate memberId,  (GetYear(CurrentTime()) - birthYear) as age;

/*
***********************************************************
 Data joins
***********************************************************
*/
join_claims = join claims_s by claimId, claims_detail_s by claimId;
join_claims_members = join join_claims by memberId, member_age by memberId;




dump join_claims_members;

/*
***********************************************************
 Data describes
***********************************************************
*/

describe claims_s;
describe member_s;
describe member_age;
describe claims_detail_s;
describe join_claims;
describe join_claims_members;