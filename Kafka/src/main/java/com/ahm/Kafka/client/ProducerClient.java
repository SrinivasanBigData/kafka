package com.ahm.Kafka.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ahm.Kafka.AHMKafkaProducer;

public class ProducerClient {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("configuration_path/producer.xml"));
		AHMKafkaProducer producer = new AHMKafkaProducer(conf);
		producer.send();
		producer.close();
	}
}
