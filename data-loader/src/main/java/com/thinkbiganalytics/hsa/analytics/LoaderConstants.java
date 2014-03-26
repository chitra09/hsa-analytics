package com.thinkbiganalytics.hsa.analytics;

public class LoaderConstants {

	// Member Table
	public static final String MEMBER_TABLE = "MEMBER";
	public static final String MEMBER_FAMILY = "M";
	public static final String DEPENDENT_FAMILY = "D";
	public static final String[] MEMBER_FAMILIES = { MEMBER_FAMILY,
			DEPENDENT_FAMILY };

	public static final String[] MEMBER_SCHEMA = { "NewMemberID", "State",
			"Zip", "Gender", "BirthYear", "HsaEffectiveDate" };
	public static final String[] DEPENDENT_SCHEMA = { "NewMemberID",
			"DependentID", "Relationship", "BirthYear", "Gender", "State",
			"Zip" };
	public static final String[] TRANSACTION_SCHEMA = { "NewMemberID",
			"Amount", "Category", "PaymentAvailableDate" };

	// Claims Table
	public static final String CLAIMS_TABLE = "CLAIMS";
	public static final String CLAIMS_FAMILY = "D";
	public static final String[] CLAIMS_FAMILIES = { CLAIMS_FAMILY };

	public static final String[] CLAIMS_SCHEMA = { "NewClaimID", "NewMemberID",
			"DependentServiced", "ClaimType", "DateReceived", "DateProcessed",
			"ServiceStart", "ServiceEnd", "RepricedAmount",
			"PatientResponsibilityAmount" };
	public static final String[] CLAIMS_DETAILS_SCHEMA = { "NewClaimID",
			"CPTCode" };

}
