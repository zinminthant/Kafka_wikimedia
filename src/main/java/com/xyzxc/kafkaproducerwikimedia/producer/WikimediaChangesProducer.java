package com.xyzxc.kafkaproducerwikimedia.producer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

public class WikimediaChangesProducer {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		String bootstrapServers = "127.0.0.1:9092";
		String topic = "wikimedia.recentchange";
		String url = "https://stream.wikimedia.org/v2/stream/recentchange";

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        
        EventHandler eventHandler=new WikimediaChangeHandler(producer, topic);
        
        EventSource.Builder builder=new EventSource.Builder(eventHandler,URI.create(url));
        
        EventSource eventSource = builder.build();
        
        eventSource.start();
        
        TimeUnit.MINUTES.sleep(10);

        
	}

}
