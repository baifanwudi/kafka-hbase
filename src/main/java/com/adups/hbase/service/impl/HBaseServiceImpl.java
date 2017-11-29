package com.adups.hbase.service.impl;

import com.adups.hbase.service.IHBaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author allen
 * @date 23/11/2017.
 */

@Service
public class HBaseServiceImpl implements IHBaseService {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private Configuration configuration;

	@Override
	public void createTable(String tableName, String... families) {
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Admin admin = connection.getAdmin()) {
			for (String family : families) {
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			}
			if (admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println("Table Exists");
				logger.info("Table:[" + tableName + "] Exists");
			} else {
				admin.createTable(tableDescriptor);
				System.out.println("Create table Successfully!!!Table Name:[" + tableName + "]");
				logger.info("Create table Successfully!!!Table Name:[" + tableName + "]");
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void deleteTable(String tableName) {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Admin admin = connection.getAdmin()) {
			TableName table = TableName.valueOf(tableName);
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				logger.info("[" + tableName + "] is not existed. Delete failed!");
				return;
			}
			admin.disableTable(table);
			admin.deleteTable(table);
			System.out.println("delete table " + tableName + " successfully!");
			logger.info("delete table " + tableName + " successfully!");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void putRowValue(String tableName, String rowKey, String familyColumn, String columnName, String value) {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnName), Bytes.toBytes(value));
			table.put(put);
			logger.info("update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",column:" + columnName + ",value:" + value + " successfully!");
			System.out.println("Update table success");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void putRowValueBatch(String tableName, String rowKey, String familyColumn, List<String> columnNames, List<String> values) {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Put put = new Put(Bytes.toBytes(rowKey));
			for (int j = 0; j < columnNames.size(); j++) {
				put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnNames.get(j)), Bytes.toBytes(values.get(j)));
			}
			table.put(put);
			logger.info("update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",columns:" + columnNames + ",values:" + values + " successfully!");
			System.out.println("Update table success");

		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void putRowValueBatch(String tableName, String rowKey, String familyColumn, Map<String, String> columnValues) {
		logger.info("begin to update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",columnValues:" + columnValues.toString());
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Put put = new Put(Bytes.toBytes(rowKey));
			for (Map.Entry<String, String> entry : columnValues.entrySet()) {
				put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
			}
			table.put(put);
			logger.info("update table:" + tableName + ",rowKey:" + rowKey + " successfully!");
			System.out.println("Update table success");

		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public List<Cell> scanRegexRowKey(String tableName, String regexKey) {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Scan scan = new Scan();
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexKey));
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				return r.listCells();
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		return null;
	}

	@Override
	public void deleteAllColumn(String tableName, String rowKey) {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Delete delAllColumn = new Delete(Bytes.toBytes(rowKey));
			table.delete(delAllColumn);
			System.out.println("Delete AllColumn Success");
			logger.info("Delete rowKey:" + rowKey + "'s all Columns Successfully");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void deleteColumn(String tableName, String rowKey, String familyName, String columnName) {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Delete delColumn = new Delete(Bytes.toBytes(rowKey));
			delColumn.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			table.delete(delColumn);
			System.out.println("Delete Column Success");
			logger.info("Delete rowKey:" + rowKey + "'s Column:" + columnName + " Successfully");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}
}
