package com.adups;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author  allen
 */
@SpringBootApplication
public class KafkaHBaseApplication {

	public static void main(String[] args) throws Exception {
//		System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
		SpringApplication.run(KafkaHBaseApplication.class, args);
//		ApplicationContext applicationContext =  SpringApplication.run(KafkaHbaseApplication.class,args);
//
//		System.out.println(applicationContext.getBean(Configuration.class));
//		Configuration conf=applicationContext.getBean(Configuration.class);
//		System.out.println(conf.get("hbase.zookeeper.quorum"));
	}
}
