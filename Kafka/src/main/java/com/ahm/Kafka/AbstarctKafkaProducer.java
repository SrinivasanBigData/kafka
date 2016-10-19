package com.ahm.Kafka;

import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;

import com.ahm.kafka.constant.IproducerConstant;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstarctKafkaProducer<K, V> implements AutoCloseable,
		IproducerConstant {

	@NonNull
	public Configuration config;

	private Properties properties;

	private Producer<K, V> producer;

	public abstract Iterator<V> sendMsgIterator();

	private void setProducerProperties() {
		properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				config.get(BOOTSTRAP_SERVERS));
		properties.setProperty(ProducerConfig.ACKS_CONFIG,
				config.get(ACKS, "all"));
		properties.setProperty(ProducerConfig.RETRIES_CONFIG,
				config.get(RETRIES, "3"));
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,
				config.get(BATCH_SIZE, "128"));
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,
				config.get(LINGER_MS, "1"));
		properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,
				config.get(BUFFER_MEMORY, "33554432"));
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,
				config.get(COMPRESSION_TYPE, "snappy"));
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				config.get(KEY_SERIALIZER_CLASS));
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				config.get(VALUE_SERIALIZER_CLASS));
	}

	private Producer<K, V> getProducer() {
		this.setProducerProperties();
		log.info("Establishing Kafka Producer Connection");
		producer = new KafkaProducer<K, V>(properties);
		log.info(" Kafka Producer Connection established.");
		return producer;
	}

	public void send() {
		if (producer == null) {
			this.getProducer();
		}
		Iterator<V> iterator = sendMsgIterator();
		while (iterator.hasNext()) {
			V v = iterator.next();
			ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(
					this.config.get(TOPICNAME), v);
			producer.send(producerRecord);
		}
	}

	@Override
	public void close() throws Exception {
		if (producer != null)
			producer.close();
	}
}
