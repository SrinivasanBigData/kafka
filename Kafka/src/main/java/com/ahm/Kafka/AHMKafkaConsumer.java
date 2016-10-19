package com.ahm.Kafka;

import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AHMKafkaConsumer extends AbstractKafkaConsumer<String, String> {

	private KafkaConsumer<String, String> kafkaConsumer;

	public AHMKafkaConsumer(Configuration config, String name,
			boolean isInterruptible) {
		super(config, name, isInterruptible);
	}

	@Override
	public void doWork() {
		kafkaConsumer = getKafkaConsumer();
		kafkaConsumer.subscribe(Collections.singletonList(this.config
				.get(TOPICNAME)));
		Iterator<ConsumerRecord<String, String>> records = kafkaConsumer.poll(
				this.config.getLong(POLL, 1000)).iterator();
		while (records.hasNext()) {
			ConsumerRecord<java.lang.String, java.lang.String> record = records
					.next();
			System.out.println("Received message: (" + record.key() + ", "
					+ record.value() + ") at offset " + record.offset());

		}
	}

}