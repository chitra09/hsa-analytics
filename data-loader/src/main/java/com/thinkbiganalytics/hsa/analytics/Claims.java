package com.thinkbiganalytics.hsa.analytics;

/**
 * Claims Data
 * 
 * NewClaimID,NewMemberID,DependentServiced,ClaimType,DateReceived,
 * DateProcessed
 * ,ServiceStart,ServiceEnd,RepricedAmount,PatientResponsibilityAmount
 * 
 * Claims Details
 * 
 * NewClaimID,CPTCode
 * 
 */

public class Claims {

	// Claims
	private String claimID;
	private String memberID;
	private String dependentServiced;
	private String claimType;
	private String dateRcvd;
	private String dateProcessed;
	private String serviceStart;
	private String serviceEnd;
	private double repricedAmt;
	private double finalAmt;
	private boolean isValid;
	private DataType type;

	// Claims Details
	private String cptCode;

	public Claims(String row) {
		String fields[] = row.split(",", -1);
		// Check for header information in the data set
		if (fields[0].equalsIgnoreCase(LoaderConstants.CLAIMS_SCHEMA[0])
				|| fields[0]
						.equalsIgnoreCase(LoaderConstants.CLAIMS_DETAILS_SCHEMA[0])) {
			isValid = false;
		} else {
			isValid = true;
			if (fields.length == LoaderConstants.CLAIMS_SCHEMA.length) {
				setType(DataType.CLAIMS);
				setClaimID(fields[0]);
				setMemberID(fields[1]);
				setDependentServiced(fields[2]);
				setClaimType(fields[3]);
				setDateRcvd(fields[4]);
				setDateProcessed(fields[5]);
				setServiceStart(fields[6]);
				setServiceEnd(fields[7]);
				setRepricedAmt(fields[8]);
				setFinalAmt(fields[9]);
			} else if (fields.length == LoaderConstants.CLAIMS_DETAILS_SCHEMA.length) {
				setType(DataType.CLAIMS_DETAILS);
				setClaimID(fields[0]);
				setCptCode(fields[1]);
			}

		}
	}

	public String getClaimID() {
		return claimID;
	}

	public void setClaimID(String claimID) {
		this.claimID = claimID;
	}

	public String getMemberID() {
		return memberID;
	}

	public void setMemberID(String memberID) {
		this.memberID = memberID;
	}

	public String getDependentServiced() {
		return dependentServiced;
	}

	public void setDependentServiced(String dependentServiced) {
		this.dependentServiced = dependentServiced;
	}

	public String getClaimType() {
		return claimType;
	}

	public void setClaimType(String claimType) {
		this.claimType = claimType;
	}

	public String getDateRcvd() {
		return dateRcvd;
	}

	public void setDateRcvd(String dateRcvd) {
		this.dateRcvd = dateRcvd;
	}

	public String getDateProcessed() {
		return dateProcessed;
	}

	public void setDateProcessed(String dateProcessed) {
		this.dateProcessed = dateProcessed;
	}

	public String getServiceStart() {
		return serviceStart;
	}

	public void setServiceStart(String serviceStart) {
		this.serviceStart = serviceStart;
	}

	public String getServiceEnd() {
		return serviceEnd;
	}

	public void setServiceEnd(String serviceEnd) {
		this.serviceEnd = serviceEnd;
	}

	public double getRepricedAmt() {
		return repricedAmt;
	}

	public void setRepricedAmt(String repricedAmt) {
		this.repricedAmt = Double.parseDouble(repricedAmt);
	}

	public double getFinalAmt() {
		return finalAmt;
	}

	public void setFinalAmt(String finalAmt) {
		this.finalAmt = Double.parseDouble(finalAmt);
	}

	public String getCptCode() {
		return cptCode;
	}

	public void setCptCode(String cptCode) {
		this.cptCode = cptCode;
	}

	public boolean isValid() {
		return isValid;
	}

	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}

}
