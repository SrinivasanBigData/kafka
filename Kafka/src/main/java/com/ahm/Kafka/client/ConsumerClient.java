package com.ahm.Kafka.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ahm.Kafka.AHMKafkaConsumer;

public class ConsumerClient {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.addResource(new Path("configuration_path/consumer.xml"));
		AHMKafkaConsumer consumer = new AHMKafkaConsumer(conf, "Ahm_consumer",
				false);
		consumer.start();
	}

}
