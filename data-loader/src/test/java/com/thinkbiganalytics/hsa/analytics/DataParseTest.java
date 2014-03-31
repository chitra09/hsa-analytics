package com.thinkbiganalytics.hsa.analytics;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * The member data is of the form
 * NewMemberID,State,Zip,Gender,BirthYear,HsaEffectiveDate
 * 
 * The dependent data is of the form
 * NewMemberID,DependentID,Relationship,BirthYear,Gender,State,Zip
 * 
 * Claims Data is of the form
 * NewClaimID,NewMemberID,DependentServiced,ClaimType,DateReceived,
 * DateProcessed
 * ,ServiceStart,ServiceEnd,RepricedAmount,PatientResponsibilityAmount
 * 
 * Claims Details is of the form NewClaimID,CPTCode
 * 
 */
public class DataParseTest {

	@Test
	public void parseDependentData() {
		String data = "738,00,Self,1972,M,,";
		Member member = new Member(data);
		assertEquals(DataType.DEPENDENT, member.getType());
		assertEquals("738", member.getMemberID());
		assertEquals("00", member.getdID());
		assertEquals("Self", member.getRelationship());
		assertEquals("1972", member.getdBirthYear());
		assertEquals("M", member.getdGender());
		assertEquals("", member.getdState());
		assertEquals("", member.getdZip());
	}

	@Test
	public void parseMemberData() {
		String data = "6,AR,71854,F,1960,2012-01-01 00:00:00.000";
		Member member = new Member(data);
		assertEquals(DataType.MEMBER, member.getType());
		assertEquals("6", member.getMemberID());
		assertEquals("AR", member.getState());
		assertEquals("71854", member.getZip());
		assertEquals("F", member.getGender());
		assertEquals("1960", member.getBirthYear());
		assertEquals("2012-01-01 00:00:00.000", member.getHsaEffectiveDate());
	}

	@Test
	public void parseTransactionData() {
		String data = "7252,-30.200,DistNormal,2012-11-14 00:00:00.000";
		Member member = new Member(data);
		assertEquals(DataType.TRANSACTION, member.getType());
		assertEquals("7252", member.getMemberID());
		assertEquals("-30.200", member.getAmount());
		assertEquals("DistNormal", member.getCategory());
		assertEquals("2012-11-14 00:00:00.000",
				member.getPaymentAvailableDate());
	}

	@Test
	public void parseClaimsData() {
		String data = "6,254,02,Professional,2013-12-20 16:37:42.000,2013-12-20 16:38:50.753,2013-01-19 00:00:00.000,2013-01-20 00:00:00.000,108.68,95.88,Rx";
		Claims claims = new Claims(data);
		assertEquals(DataType.CLAIMS, claims.getType());
		assertEquals("6", claims.getClaimID());
		assertEquals("254", claims.getMemberID());
		assertEquals("02", claims.getDependentServiced());
		assertEquals("Professional", claims.getClaimType());
		assertEquals("2013-12-20 16:37:42.000", claims.getDateRcvd());
		assertEquals("2013-12-20 16:38:50.753", claims.getDateProcessed());
		assertEquals("2013-01-19 00:00:00.000", claims.getServiceStart());
		assertEquals("2013-01-20 00:00:00.000", claims.getServiceEnd());
		assertEquals("108.68", claims.getRepricedAmt());
		assertEquals("95.88", claims.getFinalAmt());
		assertEquals("Rx", claims.getCptCode());
	}
}
