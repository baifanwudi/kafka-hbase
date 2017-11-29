package com.adups.kafka.consumer.impl;

import com.adups.hbase.config.BaseConfig;
import com.adups.hbase.service.IHBaseService;
import com.adups.kafka.bean.CheckInfo;
import com.adups.kafka.consumer.AbstractConsumer;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author allen
 * @date 23/11/2017.
 */

@Component
public class CheckConsumer extends AbstractConsumer<CheckInfo> {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	IHBaseService ihBaseService;

	@Override
	@KafkaListener(topics = {"ota_check"})
	public void listen(ConsumerRecord<?, ?> record) {
		System.out.printf("offset = %d,topic= %s,partition=%s,key =%s,value=%s\n", record.offset(), record.topic(), record.partition(), record.key(), record.value());
		logger.info("value is: "+record.value());
		CheckInfo checkInfo = JSON.parseObject(record.value().toString(), CheckInfo.class);
		updateOrInsert(checkInfo);
	}

	@Override
	public void updateOrInsert(CheckInfo checkInfo) {
		Map<String, String> columnValues = new HashMap<>(8);
		Long productId = checkInfo.getProductId();
		Integer deltaId = checkInfo.getDeltaId();
		String mid = checkInfo.getMid();
		String rowKeyRegex = rowKeyRegex(productId, deltaId, mid);
		List<Cell> result = ihBaseService.scanRegexRowKey(tableName, rowKeyRegex);
		columnValues.put("check_time", checkInfo.getCreateTime());
		String rowKey ;
		if (result != null) {
			//读出key,插入数据
			Cell cell = result.get(0);
			rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
			logger.info("scan the key from hbase is :"+rowKey);
		} else {
			//插入新记录
			rowKey = checkInfo.getProductId() + "+" + remainingTime() + "+" + checkInfo.getDeltaId() + "+" + checkInfo.getMid();
			logger.info(" the new rowKey is :" + rowKey);
			columnValues.put("status", BaseConfig.STATUS_CHECK_SUCCESS);
		}
		ihBaseService.putRowValueBatch(tableName, rowKey, familyColumn, columnValues);
	}
}
