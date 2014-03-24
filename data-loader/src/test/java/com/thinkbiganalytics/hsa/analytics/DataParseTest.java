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
		double epsilon = 0.000000000001;
		Member member = new Member(data);
		assertEquals(DataType.TRANSATION, member.getType());
		assertEquals("7252", member.getMemberID());
		assertEquals(-30.200, member.getAmount(), epsilon);
		assertEquals("DistNormal", member.getCategory());
		assertEquals("2012-11-14 00:00:00.000", member.getPaymentAvailableDate());
	}

}
