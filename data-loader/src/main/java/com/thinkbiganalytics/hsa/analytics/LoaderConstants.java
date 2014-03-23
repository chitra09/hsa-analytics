package com.thinkbiganalytics.hsa.analytics;

public class LoaderConstants {

	// HBase Constants
	public static final String MEMBER_TABLE = "MEMBER_TABLE";
	public static final String MEMBER_FAMILY = "M";
	public static final String DEPENDENT_FAMILY = "D";
	public static final String[] FAMILIES = { MEMBER_FAMILY, DEPENDENT_FAMILY };
	public static final String ROW_KEY = "rowkey";
	public static final String KEY_DELIMITER = ",";

	// Data Constants
	public static final String[] MEMBER_SCHEMA = { "NewMemberID,State,Zip,Gender,BirthYear,HsaEffectiveDate" };
	public static final String[] DEPENDENT_SCHEMA = { "NewMemberID,DependentID,Relationship,BirthYear,Gender,State,Zip" };

}
