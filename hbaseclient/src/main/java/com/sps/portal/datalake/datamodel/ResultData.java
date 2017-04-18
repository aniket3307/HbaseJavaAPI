package com.sps.portal.datalake.datamodel;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ResultData {
	private String code;
	private String message;
	private String profile;
	private String availableUntil;
	private String minUsage;
	private String maxUsage;
	private String avgUsage;
	private String totalUsage;
	private List<IntervalData> intervalData = new ArrayList<IntervalData>();
	
	public ResultData() {
	}

	public ResultData(String code, String message, String profile, String availableUntil, String minUsage,
			String maxUsage, String avgUsage, String totalUsage, List<IntervalData> intervalData) {
		this.code = code;
		this.message = message;
		this.profile = profile;
		this.availableUntil = availableUntil;
		this.minUsage = minUsage;
		this.maxUsage = maxUsage;
		this.avgUsage = avgUsage;
		this.totalUsage = totalUsage;
		this.intervalData = intervalData;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getProfile() {
		return profile;
	}

	public void setProfile(String profile) {
		this.profile = profile;
	}

	public String getAvailableUntil() {
		return availableUntil;
	}

	public void setAvailableUntil(String availableUntil) {
		this.availableUntil = availableUntil;
	}

	public String getMinUsage() {
		return minUsage;
	}

	public void setMinUsage(String minUsage) {
		this.minUsage = minUsage;
	}

	public String getMaxUsage() {
		return maxUsage;
	}

	public void setMaxUsage(String maxUsage) {
		this.maxUsage = maxUsage;
	}

	public String getAvgUsage() {
		return avgUsage;
	}

	public void setAvgUsage(String avgUsage) {
		this.avgUsage = avgUsage;
	}

	public String getTotalUsage() {
		return totalUsage;
	}

	public void setTotalUsage(String totalUsage) {
		this.totalUsage = totalUsage;
	}

	public List<IntervalData> getIntervalData() {
		return intervalData;
	}

	public void setIntervalData(List<IntervalData> intervalData) {
		this.intervalData = intervalData;
	}
	
}
