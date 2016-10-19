package com.ahm.Kafka;

import java.util.Properties;

import kafka.utils.ShutdownableThread;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.ahm.kafka.constant.IconsumerConstant;

public abstract class AbstractKafkaConsumer<K, V> extends ShutdownableThread
		implements IconsumerConstant {

	public Configuration config;

	private Properties properties;

	private KafkaConsumer<K, V> consumer;

	public AbstractKafkaConsumer(Configuration config, String name,
			boolean isInterruptible) {
		super(name, isInterruptible);
		this.config = config;
	}

	private void setConsumerProperties() {
		properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				config.get(BOOTSTRAP_SERVERS));
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.get(GROUP_ID));
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				config.get(ENABLE_AUTO_COMMIT, "true"));
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
				config.get(AUTO_COMMIT_INTERVAL_MS, "1000"));
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
				config.get(SESSION_TIMEOUT_MS, "30000"));
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				config.get(KEY_DESERIALIZER_CLASS));
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				config.get(VALUE_DESERIALIZER_CLASS));
	}

	public KafkaConsumer<K, V> getKafkaConsumer() {
		this.setConsumerProperties();
		consumer = new KafkaConsumer<K, V>(properties);
		return consumer;
	}

	@Override
	public String name() {
		return null;
	}

	@Override
	public boolean isInterruptible() {
		return false;
	}

}
