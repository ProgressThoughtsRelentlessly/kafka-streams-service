package com.pthore.service.kafkaStreams;

import java.time.Duration;
import java.util.Properties;

import javax.annotation.PreDestroy;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

@SpringBootApplication
public class PthoreKafkaStreamsService1Application implements ApplicationListener<ApplicationReadyEvent>{
	
	@Autowired 
	@Qualifier("kafkaProperties")
	private Properties properties;
	
	@Autowired
	private Topology topology;
	
	private KafkaStreams streams;
	
	
	@Bean ("jsonMapper")
	@Primary
	public JsonMapper objectMapper() {
		return JsonMapper.builder().configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
				.addModule(new ParameterNamesModule())
				.addModule(new Jdk8Module())
				.addModule(new JavaTimeModule())
		.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).build();
	}
	
	@PreDestroy
	public void onShutdown() {
		if(streams != null)
			streams.close(Duration.ofMillis(200));
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		
		streams = new KafkaStreams(topology, properties);
		streams.start();
	}
	
	public static void main(String[] args) {
		SpringApplication.run(PthoreKafkaStreamsService1Application.class, args);
		
	}
}
