package com.thinkbiganalytics.hsa.analytics;

/**
 * The member data is of the form
 * NewMemberID,State,Zip,Gender,BirthYear,HsaEffectiveDate
 * 
 * The dependent data is of the form
 * NewMemberID,DependentID,Relationship,BirthYear,Gender,State,Zip
 * 
 * The transaction data is of the form
 * NewMemberID,Amount,Category,PaymentAvailableDate
 * 
 */
public class Member {

	private String memberID;
	private String state;
	private String zip;
	private String gender;
	private String birthYear;
	private String hsaEffectiveDate;
	private boolean isMember;
	private boolean isValid;
	private DataType type;

	// Dependent Specific Info
	private String dID;
	private String relationship;
	private String dBirthYear;
	private String dGender;
	private String dState;
	private String dZip;

	// Transaction Specific Info
	private double amount;
	private String category;
	private String paymentAvailableDate;

	public Member(String row) {
		String fields[] = row.split(",", -1);
		// Check for header information in the data set
		if (fields[0].equalsIgnoreCase(LoaderConstants.MEMBER_SCHEMA[0])
				|| fields[0]
						.equalsIgnoreCase(LoaderConstants.DEPENDENT_SCHEMA[0])
				|| fields[0]
						.equalsIgnoreCase(LoaderConstants.TRANSACTION_SCHEMA[0])) {
			isValid = false;
		} else {
			isValid = true;
			if (fields.length == LoaderConstants.MEMBER_SCHEMA.length) {
				setType(DataType.MEMBER);
				setMemberID(fields[0]);
				setState(fields[1]);
				setZip(fields[2]);
				setGender(fields[3]);
				setBirthYear(fields[4]);
				setHsaEffectiveDate(fields[5]);
			} else if (fields.length == LoaderConstants.DEPENDENT_SCHEMA.length) {
				setType(DataType.DEPENDENT);
				setMemberID(fields[0]);
				setdID(fields[1]);
				setRelationship(fields[2]);
				setdBirthYear(fields[3]);
				setdGender(fields[4]);
				setdState(fields[5]);
				setdZip(fields[6]);
			} else if (fields.length == LoaderConstants.TRANSACTION_SCHEMA.length) {
				setType(DataType.TRANSATION);
				setMemberID(fields[0]);
				setAmount(fields[1]);
				setCategory(fields[2]);
				setPaymentAvailableDate(fields[3]);
			}
		}
	}

	public String getBirthYear() {
		return birthYear;
	}

	public void setBirthYear(String birthYear) {
		this.birthYear = birthYear;
	}

	public String getMemberID() {
		return memberID;
	}

	public void setMemberID(String memberID) {
		this.memberID = memberID;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getHsaEffectiveDate() {
		return hsaEffectiveDate;
	}

	public void setHsaEffectiveDate(String hsaEffectiveDate) {
		this.hsaEffectiveDate = hsaEffectiveDate;
	}

	public String getdID() {
		return dID;
	}

	public void setdID(String dID) {
		this.dID = dID;
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

	public String[] getSchema() {
		return (isMember == true) ? LoaderConstants.MEMBER_SCHEMA
				: LoaderConstants.DEPENDENT_SCHEMA;
	}

	public boolean isValid() {
		return isValid;
	}

	public int getLength() {
		return (isMember == true) ? LoaderConstants.MEMBER_SCHEMA.length
				: LoaderConstants.DEPENDENT_SCHEMA.length;
	}

	public String getdBirthYear() {
		return dBirthYear;
	}

	public void setdBirthYear(String dBirthYear) {
		this.dBirthYear = dBirthYear;
	}

	public String getdGender() {
		return dGender;
	}

	public void setdGender(String dGender) {
		this.dGender = dGender;
	}

	public String getdState() {
		return dState;
	}

	public void setdState(String dState) {
		this.dState = dState;
	}

	public String getdZip() {
		return dZip;
	}

	public void setdZip(String dZip) {
		this.dZip = dZip;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(String amount) {
		this.amount = Double.parseDouble(amount);
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getPaymentAvailableDate() {
		return paymentAvailableDate;
	}

	public void setPaymentAvailableDate(String paymentAvailableDate) {
		this.paymentAvailableDate = paymentAvailableDate;
	}
	
	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}
}
