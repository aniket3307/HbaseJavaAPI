package com.sps.portal.datalake.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sps.portal.datalake.datamodel.ResultData;
import com.sps.portal.datalake.queries.Queries;

@Service
public class PortalService {
	
	@Autowired
	private Queries queryObj;


	public ResultData getResult(String acc_no, String from_date, String to_date, String frequency) throws Exception{

		ResultData result = null;

		if (frequency.equals("halfHourly")){
			result = queryObj.getHalfHourlyResultData(acc_no, from_date, to_date);
		} else if(frequency.equals("daily")){
			result = queryObj.getDailyResultData(acc_no, from_date, to_date);
		} else if(frequency.equals("monthly")){
			result = queryObj.getMonthlyResultData(acc_no, from_date, to_date);
		} else {
			throw new Exception("Incorrect URL: /" + frequency);
		}

		return result;
	}

}
