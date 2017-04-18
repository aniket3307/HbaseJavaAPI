package com.sps.portal.datalake.util;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

public class Utilities {
	public static DecimalFormat precision = new DecimalFormat("#0.000");
	public static String zkQuorum = "inairbimapp177.corp.capgemini.com,inairbimapp176.corp.capgemini.com," +
			"inairbimapp175.corp.capgemini.com";
	public static String zkQuorumSPS = "cghwd1.in.spdigital.io,cghwd2.in.spdigital.io,cghwd3.in.spdigital.io";

	public static HTable getHBaseTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", zkQuorum);
		config.set("hbase.zookeeper.property.clientport", "2181");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");

		HBaseAdmin.checkHBaseAvailable(config);

		HTable table = new HTable(config, tableName);

		return table;
	}
	
	public static SingleColumnValueFilter getColumnFilterGoE(String colFamily, String colName, String value){
		return new SingleColumnValueFilter(
				Bytes.toBytes(colFamily),
				Bytes.toBytes(colName),
				CompareOp.GREATER_OR_EQUAL,
				Bytes.toBytes(value)
				);
	}

	public static SingleColumnValueFilter getColumnFilterLoE(String colFamily, String colName, String value){
		return new SingleColumnValueFilter(
				Bytes.toBytes(colFamily),
				Bytes.toBytes(colName),
				CompareOp.LESS_OR_EQUAL,
				Bytes.toBytes(value)
				);
	}
	
	public static String getTime(int i){
		int hour = i/2;
		int mins = i%2;
		String prefix, postfix;

		if( hour < 10){
			prefix = "0" + hour;
		} else {
			prefix = Integer.toString(hour);
		}

		if(mins == 0) postfix = "00";
		else postfix = "30";

		return prefix + ":" + postfix;
	}
	
	public static String getDate(String date){
		String yyyy = date.split("/")[2];
		String mm = date.split("/")[1];
		String dd = date.split("/")[0];

		return yyyy + "-" + mm + "-" + dd;
	}
}
