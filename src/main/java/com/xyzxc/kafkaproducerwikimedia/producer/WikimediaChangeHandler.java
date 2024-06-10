package com.xyzxc.kafkaproducerwikimedia.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler{
	
	KafkaProducer<String,String> kafkaProducer;
	
	String topic;
	
	private final Logger logger=LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());
	
	
	public WikimediaChangeHandler(KafkaProducer<String,String> kafkaProducer,String topic) {
		
		this.kafkaProducer=kafkaProducer;
		this.topic=topic;
		
	}

	@Override
	public void onOpen() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onClosed() throws Exception {
		kafkaProducer.close();
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		
		logger.info (messageEvent.getData());
		kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
		
	}

	@Override
	public void onComment(String comment) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(Throwable t) {
		logger.error("error in stream reading",t);
		
	}

}
