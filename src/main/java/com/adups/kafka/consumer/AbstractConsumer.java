package com.adups.kafka.consumer;

import com.adups.hbase.config.BaseConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author allen
 * @date 27/11/2017.
 */
public abstract class AbstractConsumer<T> {

	protected String tableName= BaseConfig.TABLE_NAME;

	protected String familyColumn=BaseConfig.FAMILY_COLUMN;

	/**
	 * kafka消费监控
	 * @param record
	 */
	public  abstract void listen(ConsumerRecord<?, ?> record);

	/**
	 * 消费记录,向hbase表做操作
	 * @param rowKeyRegex
	 * @param t
	 */
	public abstract void updateOrInsert(T t);

	protected String rowKeyRegex(Long productId,Integer deltaId,String mid){
		return "^"+productId+"\\+\\d*\\+"+deltaId+"\\+"+mid+"$";
	}

	protected   long remainingTime(){
		return Long.MAX_VALUE-System.currentTimeMillis();
	}

}
