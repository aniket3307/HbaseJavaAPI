package com.sps.portal.datalake.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import com.google.protobuf.ServiceException;
import com.sps.portal.datalake.datamodel.IntervalData;
import com.sps.portal.datalake.datamodel.ResultData;
import com.sps.portal.datalake.util.Utilities;

@Component
public class Queries {

	public ResultData getHalfHourlyResultData(String acc_no, String from_date, String to_date) throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException{

		HTable table = Utilities.getHBaseTable("SPS_TEST_REST");
		Filter regExFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^" + acc_no));
		Filter prefixFilter = new PrefixFilter(Bytes.toBytes(acc_no));
		FilterList listOfFilters = new FilterList();
		listOfFilters.addFilter(regExFilter);
		//listOfFilters.addFilter(prefixFilter);
		Scan scan = new Scan();
		scan.setFilter(listOfFilters);
		ResultScanner scanner = table.getScanner(scan);

		String AVL_DATE = getMaxDateAvailable(scanner);

		SingleColumnValueFilter fromDateFilter = Utilities.getColumnFilterGoE("0", "VALUEDAY", from_date);
		SingleColumnValueFilter toDateFilter = Utilities.getColumnFilterLoE("0", "VALUEDAY", to_date);

		listOfFilters.addFilter(fromDateFilter);
		listOfFilters.addFilter(toDateFilter);
		scan.setFilter(listOfFilters);
		/*scan.setStartRow(Bytes.toBytes(acc_no + "_" + from_date)).setStopRow(Bytes.toBytes(acc_no + "_" + to_date));*/
		scanner = table.getScanner(scan);

		Map<String, Map<String, String>> resultSet = new HashMap<String, Map<String,String>>();

		for(Result result = scanner.next(); result != null; result = scanner.next()){
			Map<String, String> mapOfData = new HashMap<String, String>();        	
			List<Cell> listOfCells = new ArrayList<Cell>();

			listOfCells = result.listCells();
			for(Cell c: listOfCells){
				byte[] family = c.getFamily();
				byte[] qualifier = c.getQualifier();

				if(new String(qualifier).equals("_0")) continue;

				mapOfData.put(new String(qualifier), new String(result.getValue(family, qualifier)));
			}

			String date = new String(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("VALUEDAY")));

			resultSet.put(date, mapOfData);

		}

		int numOfRows = 0;
		String PROFILE = null;
		double MIN_USAGE = 0;
		double MAX_USAGE = 0;
		double AVG_USAGE = 0;
		double TOTAL_USAGE = 0;
		List<IntervalData> halfHourlyData = new ArrayList<IntervalData>();

		String[] dates = resultSet.keySet().toArray(new String[resultSet.size()]);
		Arrays.sort(dates);

		ResultData result = null;

		for(String date: dates){
			numOfRows++;

			Map<String,String> rowData = resultSet.get(date);
			String[] columns = rowData.keySet().toArray(new String[rowData.size()]);
			Arrays.sort(columns);

			List<String> intervalDataAsList = new ArrayList<String>();
			for(int i=6; i<=columns.length-2; i++){
				intervalDataAsList.add(rowData.get(columns[i]));
			}

			String sum = rowData.get("SUM");
			String avg = rowData.get("AVG");
			String min = rowData.get("MIN");
			String max = rowData.get("MAX");

			if(numOfRows == 1){
				MIN_USAGE = Double.parseDouble(min);
				MAX_USAGE = Double.parseDouble(max);
				AVG_USAGE = Double.parseDouble(avg);
				TOTAL_USAGE = Double.parseDouble(sum);
				PROFILE = rowData.get("PROFILE");
			} else {
				MIN_USAGE = (Double.parseDouble(min) < MIN_USAGE) ? Double.parseDouble(min) : MIN_USAGE;
				MAX_USAGE = (Double.parseDouble(max) > MAX_USAGE) ? Double.parseDouble(max) : MAX_USAGE;
				TOTAL_USAGE = TOTAL_USAGE + Double.parseDouble(sum);
				AVG_USAGE = TOTAL_USAGE/(48*numOfRows);
			}

			for(int i=0; i<48; i++){
				halfHourlyData.add(new IntervalData(Utilities.getTime(i), intervalDataAsList.get(i), date));
			}
		}

		result = new ResultData("000", "Success", PROFILE, AVL_DATE,
				Utilities.precision.format(MIN_USAGE), Utilities.precision.format(MAX_USAGE), Utilities.precision.format(AVG_USAGE),
				Utilities.precision.format(TOTAL_USAGE), halfHourlyData);

		return result;

	}

	public ResultData getDailyResultData(String acc_no, String from_date, String to_date) throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException{

		HTable table = Utilities.getHBaseTable("SPS_TEST_REST");
		Filter regExFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^" + acc_no));
		Filter prefixFilter = new PrefixFilter(Bytes.toBytes(acc_no));
		FilterList listOfFilters = new FilterList();
		/*listOfFilters.addFilter(regExFilter);*/
		listOfFilters.addFilter(prefixFilter);
		Scan scan = new Scan();
		scan.setFilter(listOfFilters);
		ResultScanner scanner = table.getScanner(scan);

		String AVL_DATE = getMaxDateAvailable(scanner);

		SingleColumnValueFilter fromDateFilter = Utilities.getColumnFilterGoE("0", "VALUEDAY", from_date);
		SingleColumnValueFilter toDateFilter = Utilities.getColumnFilterLoE("0", "VALUEDAY", to_date);

		listOfFilters.addFilter(fromDateFilter);
		listOfFilters.addFilter(toDateFilter);
		scan.setFilter(listOfFilters);
		scanner = table.getScanner(scan);

		Map<String, Map<String, String>> resultSet = new HashMap<String, Map<String,String>>();

		for(Result result = scanner.next(); result != null; result = scanner.next()){
			Map<String, String> mapOfData = new HashMap<String, String>();        	
			List<Cell> listOfCells = new ArrayList<Cell>();

			listOfCells = result.listCells();
			for(Cell c: listOfCells){
				byte[] family = c.getFamily();
				byte[] qualifier = c.getQualifier();

				if(new String(qualifier).equals("PROFILE") || new String(qualifier).equals("SUM"))
					mapOfData.put(new String(qualifier), new String(result.getValue(family, qualifier)));
				else continue;
			}

			String date = new String(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("VALUEDAY")));

			resultSet.put(date, mapOfData);

		}

		int numOfRows = 0;
		String PROFILE = null;
		double MIN_USAGE = 0;
		double MAX_USAGE = 0;
		double AVG_USAGE = 0;
		double TOTAL_USAGE = 0;
		List<IntervalData> listOfIntervals= new ArrayList<IntervalData>();

		String[] dates = resultSet.keySet().toArray(new String[resultSet.size()]);
		Arrays.sort(dates);

		ResultData result = null;

		for(String date: dates){
			numOfRows++;

			Map<String,String> rowData = resultSet.get(date);
			String[] columns = rowData.keySet().toArray(new String[rowData.size()]);
			Arrays.sort(columns);

			/*List<String> intervalDataAsList = new ArrayList<String>();*/

			String sum = rowData.get("SUM");

			if(numOfRows == 1){
				MIN_USAGE = Double.parseDouble(sum);
				MAX_USAGE = Double.parseDouble(sum);
				AVG_USAGE = Double.parseDouble(sum);
				TOTAL_USAGE = Double.parseDouble(sum);
				PROFILE = rowData.get("PROFILE");
			} else {
				MIN_USAGE = (Double.parseDouble(sum) < MIN_USAGE) ? Double.parseDouble(sum) : MIN_USAGE;
				MAX_USAGE = (Double.parseDouble(sum) > MAX_USAGE) ? Double.parseDouble(sum) : MAX_USAGE;
				TOTAL_USAGE = TOTAL_USAGE + Double.parseDouble(sum);
				AVG_USAGE = TOTAL_USAGE/numOfRows;
			}

			listOfIntervals.add(new IntervalData(date, sum,""));
		}

		result = new ResultData("000", "Success", PROFILE, AVL_DATE,
				Utilities.precision.format(MIN_USAGE), Utilities.precision.format(MAX_USAGE), Utilities.precision.format(AVG_USAGE),
				Utilities.precision.format(TOTAL_USAGE), listOfIntervals);

		return result;

	}

	public ResultData getMonthlyResultData(String acc_no, String from_date, String to_date) throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException{

		HTable table = Utilities.getHBaseTable("SPS_TEST_MONTHLY");
		Filter regExFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^" + acc_no));
		Filter prefixFilter = new PrefixFilter(Bytes.toBytes(acc_no));
		FilterList listOfFilters = new FilterList();
		/*listOfFilters.addFilter(regExFilter);*/
		listOfFilters.addFilter(prefixFilter);
		Scan scan = new Scan();
		scan.setFilter(listOfFilters);
		ResultScanner scanner = table.getScanner(scan);

		String AVL_DATE = getMaxDateAvailable(scanner);

		SingleColumnValueFilter fromDateFilter = Utilities.getColumnFilterGoE("0", "VALUEDAY", from_date);
		SingleColumnValueFilter toDateFilter = Utilities.getColumnFilterLoE("0", "VALUEDAY", to_date);

		listOfFilters.addFilter(fromDateFilter);
		listOfFilters.addFilter(toDateFilter);
		scan.setFilter(listOfFilters);
		scanner = table.getScanner(scan);

		Map<String, Map<String, String>> resultSet = new HashMap<String, Map<String,String>>();
		
		for(Result result = scanner.next(); result != null; result = scanner.next()){
			Map<String, String> mapOfData = new HashMap<String, String>();        	
			List<Cell> listOfCells = new ArrayList<Cell>();

			listOfCells = result.listCells();
			for(Cell c: listOfCells){
				byte[] family = c.getFamily();
				byte[] qualifier = c.getQualifier();

				if(new String(qualifier).equals("PROFILE") || new String(qualifier).equals("MONTHLY_USAGE"))
					mapOfData.put(new String(qualifier), new String(result.getValue(family, qualifier)));
				else continue;
			}

			String date = new String(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("VALUEDAY")));

			resultSet.put(date, mapOfData);

		}

		int numOfRows = 0;
		String PROFILE = null;
		double MIN_USAGE = 0;
		double MAX_USAGE = 0;
		double AVG_USAGE = 0;
		double TOTAL_USAGE = 0;
		List<IntervalData> monthlyData = new ArrayList<IntervalData>();

		String[] dates = resultSet.keySet().toArray(new String[resultSet.size()]);
		Arrays.sort(dates);
		System.out.println(dates.length);

		ResultData result = null;

		for(String date: dates){
			numOfRows++;

			Map<String,String> rowData = resultSet.get(date);
			String[] columns = rowData.keySet().toArray(new String[rowData.size()]);
			Arrays.sort(columns);

			String usage = rowData.get("MONTHLY_USAGE");

			if(numOfRows == 1){
				MIN_USAGE = Double.parseDouble(usage);
				MAX_USAGE = Double.parseDouble(usage);
				AVG_USAGE = Double.parseDouble(usage);
				TOTAL_USAGE = Double.parseDouble(usage);
				PROFILE = rowData.get("PROFILE");
			} else {
				MIN_USAGE = (Double.parseDouble(usage) < MIN_USAGE) ? Double.parseDouble(usage) : MIN_USAGE;
				MAX_USAGE = (Double.parseDouble(usage) > MAX_USAGE) ? Double.parseDouble(usage) : MAX_USAGE;
				TOTAL_USAGE = TOTAL_USAGE + Double.parseDouble(usage);
				AVG_USAGE = TOTAL_USAGE/(numOfRows);
			}
				monthlyData.add(new IntervalData(date, usage, ""));
		}

		result = new ResultData("000", "Success", PROFILE, AVL_DATE,
				Utilities.precision.format(MIN_USAGE), Utilities.precision.format(MAX_USAGE), Utilities.precision.format(AVG_USAGE),
				Utilities.precision.format(TOTAL_USAGE), monthlyData);

		return result;

	}

	public String getMaxDateAvailable(ResultScanner scanner) throws IOException{

		List<String> listOfDates = new ArrayList<String>();

		for(Result result = scanner.next(); result != null; result = scanner.next()){
			listOfDates.add(new String(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("VALUEDAY"))));
		}

		Collections.sort(listOfDates);

		return listOfDates.get(listOfDates.size() - 1);
	}
}
