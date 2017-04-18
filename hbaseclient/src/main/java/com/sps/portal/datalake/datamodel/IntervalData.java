package com.sps.portal.datalake.datamodel;

public class IntervalData {
	private String frequencyUsage;
	private String customerReading;
	private String dateField;

	public IntervalData() {
	}

	public IntervalData(String frequencyUsage, String customerReading, String dateField) {
		this.frequencyUsage = frequencyUsage;
		this.customerReading = customerReading;
		this.dateField = dateField;
	}

	public String getFrequencyUsage() {
		return frequencyUsage;
	}

	public void setFrequencyUsage(String frequencyUsage) {
		this.frequencyUsage = frequencyUsage;
	}

	public String getCustomerReading() {
		return customerReading;
	}

	public void setCustomerReading(String customerReading) {
		this.customerReading = customerReading;
	}

	public String getDateField() {
		return dateField;
	}

	public void setDateField(String dateField) {
		this.dateField = dateField;
	}

}
