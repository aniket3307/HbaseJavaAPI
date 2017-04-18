package com.sps.portal.datalake.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.sps.portal.datalake.datamodel.QueryData;
import com.sps.portal.datalake.datamodel.ResultData;
import com.sps.portal.datalake.service.PortalService;

@RestController
public class PortalRestController {
	@Autowired
	private PortalService portalService;
	
	@RequestMapping(method=RequestMethod.POST, value="/{frequency}")
	@ResponseBody
	public ResultData getData(@RequestBody QueryData query, @PathVariable String frequency) throws Exception{
		return portalService.getResult(query.getAcc_no(), query.getFrom_date(), query.getTo_date(), frequency);
	}

}
