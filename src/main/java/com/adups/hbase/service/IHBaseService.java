package com.adups.hbase.service;

import org.apache.hadoop.hbase.Cell;
import java.util.List;
import java.util.Map;

/**
 * @author allen
 * @date 23/11/2017.
 */

public interface IHBaseService {

	/**
	 * 建表
	 * @param tableName
	 * @param families
	 */
	void createTable(String tableName,String... families) ;

	/**
	 * 删除表
	 * @param tableName
	 */
	void deleteTable(String tableName);

	/**
	 * 单条put数据
	 * @param rowKey
	 * @param tableName
	 * @param familyColumn
	 * @param columnName
	 * @param value
	 */
	void putRowValue(String tableName,String rowKey,String familyColumn,String columnName,String value);

	/**
	 * 批量插入数据,用数组
	 * @param tableName
	 * @param rowKey
	 * @param familyColumn
	 * @param columnNames
	 * @param values
	 */
	void putRowValueBatch(String tableName, String rowKey, String familyColumn, List<String> columnNames, List<String> values);

	/**
	 * 批量插入数据,用HashMap
	 * @param tableName
	 * @param rowKey
	 * @param familyColumn
	 * @param columnValues
	 */
	void putRowValueBatch(String tableName, String rowKey, String familyColumn, Map<String,String> columnValues);

	/**
	 * 模糊查询某个key
	 * @param tableName
	 * @param regexKey
	 * @return
	 */
	List<Cell> scanRegexRowKey(String tableName, String regexKey);

	/**
	 * 删除rowKey那行数据,很少
	 * @param tableName
	 * @param rowKey
	 */
	void deleteAllColumn(String tableName,String rowKey);

	/**
	 * 删除某一列数据,很少用
	 * @param tableName
	 * @param rowKey
	 * @param familyName
	 * @param columnName
	 */
	void deleteColumn(String tableName,String rowKey,String familyName,String columnName);
}
