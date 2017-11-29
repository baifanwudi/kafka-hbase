package com.adups.hbase.controller;

import com.adups.hbase.service.impl.HBaseServiceImpl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author allen
 * @date 23/11/2017.
 */

@RestController
public class HBaseCommandController {

	@Autowired
	private HBaseServiceImpl hBaseService;

	//http://localhost:18080/hbase/command/table/create/
	@RequestMapping(value = "/hbase/command/table/create", method = RequestMethod.GET)
	public String createTable() throws Exception {
		hBaseService.createTable("ota_pre_record", "info");
		return "create table success!";
	}


	//http://localhost:18080/hbase/command/table/delete/
	@RequestMapping(value = "/hbase/command/table/delete/", method = RequestMethod.GET)
	public String deleteTable() throws Exception {
		hBaseService.deleteTable("ota_pre_record");
		return "delete table success!";
	}

	//http://localhost:18080/hbase/command/row/put ,%2B代表+号
	@RequestMapping(value = "/hbase/command/row/put", method = RequestMethod.GET)
	public String putRow(@RequestParam(value = "rowKey") String rowKey, @RequestParam(value = "column") String column,
	                     @RequestParam(value = "value") String value) {
		hBaseService.putRowValue("ota_pre_record", rowKey, "info", column, value);
		return "put row success";
	}

	//http://localhost:18080/hbase/command/scan/
	@RequestMapping(value = "/hbase/command/scan", method = RequestMethod.GET)
	public String scanRegexRowKey() {
		String regexKey = "^.*\\+15022176018\\+20900$";
		List<Cell> result = hBaseService.scanRegexRowKey("ota_pre_record", regexKey);
		if (null==result) {
			System.out.println("result is null");
		}
		for (Cell cell : result) {
			System.out.println("rowKey:" + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
			System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
			System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
			System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			System.out.println("Timestamp:" + cell.getTimestamp());
		}
		return "scan success";
	}

	@RequestMapping(value = "/hbase/command/delete/allColumn", method = RequestMethod.GET)
	public String deleteAllColumn(@RequestParam(value = "rowKey") String rowKey) {

		hBaseService.deleteAllColumn("ota_pre_record", rowKey);
		return "delete all column  success";
	}

	@RequestMapping(value = "/hbase/command/delete/column", method = RequestMethod.GET)
	public String deleteColumn(@RequestParam(value = "rowKey") String rowKey, @RequestParam(value = "column") String column) {
		hBaseService.deleteColumn("ota_pre_record", rowKey, "info", column);
		return "delete  column success";
	}


}
