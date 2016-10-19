package com.ahm.Kafka;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;

public class AHMKafkaProducer extends AbstarctKafkaProducer<String, String> {

	public AHMKafkaProducer(Configuration config) {
		super(config);
	}

	@Override
	public Iterator<String> sendMsgIterator() {

		return null;
	}

}
