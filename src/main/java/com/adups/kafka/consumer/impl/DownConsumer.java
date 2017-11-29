package com.adups.kafka.consumer.impl;

import com.adups.hbase.config.BaseConfig;
import com.adups.hbase.service.IHBaseService;
import com.adups.kafka.bean.DownInfo;
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
public class DownConsumer extends AbstractConsumer<DownInfo> {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	IHBaseService ihBaseService;

	@Override
	@KafkaListener(topics = {"ota_download"})
	public void listen(ConsumerRecord<?, ?> record) {
		System.out.printf("offset = %d,topic= %s,partition=%s,key =%s,value=%s\n", record.offset(), record.topic(), record.partition(), record.key(), record.value());
		logger.info("value is: "+record.value());
		DownInfo downInfo = JSON.parseObject(record.value().toString(), DownInfo.class);
		updateOrInsert(downInfo);
	}

	@Override
	public void updateOrInsert(DownInfo downInfo) {
		logger.info("DownInfo is "+downInfo.toString());
		Map<String, String> columnValues = new HashMap<>(8);
		String rowKey;
		Long productId = downInfo.getProductId();
		Integer deltaId = downInfo.getDeltaId();
		String mid = downInfo.getMid();
		String rowKeyRegex = rowKeyRegex(productId, deltaId, mid);
		List<Cell> result = ihBaseService.scanRegexRowKey(tableName, rowKeyRegex);
		if (downInfo.getDownloadStatus() == 1) {
			columnValues.put("down_time",downInfo.getCreateTime());
			columnValues.put("status",BaseConfig.STATUS_DOWN_SUCCESS);
		} else {
			columnValues.put("down_fail_time",downInfo.getCreateTime());
			columnValues.put("down_fail_status",downInfo.getDownloadStatus().toString());
			columnValues.put("status",BaseConfig.STATUS_DOWN_FAIL);
		}
		if (result != null) {
			Cell firstCell = result.get(0);
			rowKey = Bytes.toString(firstCell.getRowArray(), firstCell.getRowOffset(), firstCell.getRowLength());
			logger.info("scan the key from hbase is :"+rowKey);
			Integer status=0;
			for (Cell cell : result) {
				String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
				if ("status".equals(columnName)) {
					status = Integer.parseInt(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				}
			}
			if (status >=Integer.parseInt(BaseConfig.STATUS_DOWN_SUCCESS) ) {
			 columnValues.remove("status");
			}
		} else {
			rowKey = productId + "+" + remainingTime() + "+" + deltaId + "+" + mid;
			logger.info(" the new rowKey is :" + rowKey);
		}

		ihBaseService.putRowValueBatch(tableName, rowKey, familyColumn, columnValues);
	}
}
