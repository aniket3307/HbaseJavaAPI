package com.sps.portal.datalake.datamodel;

public class QueryData {
	private String acc_no;
	private String from_date;
	private String to_date;
	
	public QueryData() {
	}

	public QueryData(String acc_no, String from_date, String to_date) {
		this.acc_no = acc_no;
		this.from_date = from_date;
		this.to_date = to_date;
	}

	public String getAcc_no() {
		return acc_no;
	}

	public void setAcc_no(String acc_no) {
		this.acc_no = acc_no;
	}

	public String getFrom_date() {
		return from_date;
	}

	public void setFrom_date(String from_date) {
		this.from_date = from_date;
	}

	public String getTo_date() {
		return to_date;
	}

	public void setTo_date(String to_date) {
		this.to_date = to_date;
	}

}
