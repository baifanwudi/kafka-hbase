package com.adups.hbase.config;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * @author allen
 * @date 23/11/2017.
 */

@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "hbase.zookeeper")
public class HBaseConfigurationBase {

	private  Logger logger= LoggerFactory.getLogger(HBaseConfigurationBase.class);

	private String quorum;

	/**
	 * 产生HBaseConfiguration实例化Bean
	 * @return
	 */
	@Bean
	public Configuration configuration() {
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",quorum);
		logger.info("quorum is :"+quorum);
		return conf;
	}

	public String getQuorum() {
		return quorum;
	}

	public void setQuorum(String quorum) {
		this.quorum = quorum;
	}
}
