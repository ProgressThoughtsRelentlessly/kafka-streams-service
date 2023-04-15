package com.pthore.service.kafkaStreams.configs;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.pthore.service.kafkaStreams.models.Activities;
import com.pthore.service.kafkaStreams.models.AppConstants;
import com.pthore.service.kafkaStreams.models.UserActivity;

/*
kafka streams pipeline design:
	
	. divide the user_activity records based on 'userActivity' field.
	. stream from each of those output topics. draw insights from each of those streams . 
	 and output them to another set of topics.
	 
	 like 	mostViewedProfiles(top 100), 
	 		mostViewedPost (top 100), 
	 		mostSearchedKeywords (top 300), 
	 		mostPopularDomains (top 10), 
	 		peakUserActivityTime.(range)  
	  
	 so in total you would have 2 * possibleActivities + 1  topics.
	
	further design:
		
		. create a kafka consumer which maintains a cache of top 50 trending postIds or use Redis to implement it.
			probably by maintaining a priority Queue or a custom implementation of maxHeap of fixed size.
			
		. fetch those 20 records from posts-service and cache them.
		
		. calculate the most popular authors, domains, searchKeywords based on the postViews, 
		. make this a scheduled process that runs every 10 minutes.
		
		
*/
@Configuration
public class KafkaStreamsConfigs {
	
	
	@Bean(name="kafkaProperties")
	public Properties getProperties() {
		
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConstants.KAFKA_STREAMS.APPLICATION_ID);
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.KAFKA_STREAMS.BOOTSTRAP_SERVER);
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return properties ;
	}

	@Bean 
	public Topology getTopology (JsonMapper jsonMapper) {
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, String> inputTopicStream = streamsBuilder.stream(Collections.singletonList(AppConstants.KAFKA_STREAMS.INPUT_TOPIC));

// topic segregation
	inputTopicStream		
		.to(new TopicNameExtractor<String, String>() {

			@Override
			public String extract(String key, String value, RecordContext recordContext) {
				UserActivity userActivity;
				try {
					userActivity = jsonMapper.readValue(value, UserActivity.class);
					String std_activities_are_topic_names = userActivity.getActivity();
					return std_activities_are_topic_names;
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				return "default-topic-on-exception";
			}
			
		});
 
// 		mostViewedPost (top 100), 
	 KStream<String, String> mostViewedPostStream = streamsBuilder.stream(Collections.singletonList(Activities.POST_RELATED.VIEW_POST));
	 KTable<String, Long> mostViewedPostCount = mostViewedPostStream.map((key, val) -> {
				UserActivity userActivity = null;
				try {
					userActivity = jsonMapper.readValue(val, UserActivity.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return KeyValue.pair(userActivity.getPostId(), userActivity);})
			.filter((k, v) -> v != null).groupByKey()
			.count();
	 mostViewedPostCount.toStream().to("most-viewed-post-count");
	
//		mostViewedProfiles(top 100),	
	KStream<String, String> mostViewedProfileStream = streamsBuilder.stream(Collections.singletonList(Activities.USER_PROFILE_REALTED.VIEW_PROFILE));
	KTable<String, Long> mostViewedProfileCount = mostViewedProfileStream
			.mapValues((k, v) -> {
				UserActivity userActivity = null;
				try {
					userActivity = jsonMapper.readValue(v, UserActivity.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return userActivity;
			})
			.groupByKey().count();
	mostViewedProfileCount.toStream().to("most-viewed-profile-count");
	

// 		mostUpvotedPosts(top 100)
	KStream<String, String> mostUpvotedPostStream = streamsBuilder.stream(Collections.singletonList(Activities.POST_RELATED.UPVOTED));
	KTable<String, Long> mostUpvotedPostCount = mostUpvotedPostStream
			.map((k, v) -> {
				UserActivity userActivity = null;
				try {
					userActivity = jsonMapper.readValue(v, UserActivity.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return KeyValue.pair(userActivity.getPostId(), v);
			})
			.groupByKey().count();
	mostUpvotedPostCount.toStream().to("most-upvoted-post-count");
	
	
	
// 		mostSearchedKeywords (top 300), 
	KStream<String, String> mostSearchKeywordStream = streamsBuilder.stream(Collections.singletonList(Activities.SEARCH_RELATED.SEARCH_POST));
	KTable<String, Long> mostSearchedKeywordsCount = mostSearchKeywordStream.flatMapValues((k, v) -> {
		UserActivity userActivity = null;
		try {
			userActivity = jsonMapper.readValue(v, UserActivity.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String searchSentence  = userActivity.getSearchSentence().toLowerCase()
				.replaceAll("\\bis\\b|\\bwas\\b|\\bthe\\b|\\bor\\b|\\band\\b|\\bto\\b|\\bi\\b|\\byou\\b|\\bhere\\b|\\bthere\\b", "");
		String[] words = searchSentence.split(" ");
		return Arrays.asList(words);
		
	}).map((k, v)-> {
		return KeyValue.pair(v, v);
	})
	.groupByKey()
	.count();
		
	mostSearchedKeywordsCount.toStream().to("most-searched-keyword-count");
		
		
// 		mostPopularDomains (top 10), 
	KStream<String, String> mostPopularDomainStream = streamsBuilder.stream(Collections.singletonList(Activities.POST_RELATED.CHOOSEN_DOMAIN));
	KTable<String, Long> mostPopularDomainCount = mostPopularDomainStream
			.map((k, v) -> {
				UserActivity userActivity = null;
				try {
					userActivity = jsonMapper.readValue(v, UserActivity.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return KeyValue.pair(userActivity.getDomain(), v);
			})
			.groupByKey().count();
	mostPopularDomainCount.toStream().to("most-popular-domain-count");	
			
// 		peakUserActivityTime.(range) 		
	KStream<String, String> peakUserActivityStream = streamsBuilder.stream(Collections.singletonList(AppConstants.KAFKA_STREAMS.INPUT_TOPIC));
	KTable<String, Long> peakUserActivityRange = peakUserActivityStream
			.map((k, v) -> {
				UserActivity userActivity = null;
				try {
					userActivity = jsonMapper.readValue(v, UserActivity.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				LocalDateTime timestamp = userActivity.getActivityTimestamp();
				LocalTime localTime = timestamp.toLocalTime();
				
				if(localTime.compareTo(Activities.ACTIVITY_TIMMINGS.BEFORE_04_AM) < 1) {
					return KeyValue.pair(Activities.ACTIVITY_TIMMINGS_NAMES.BEFORE_04_AM, v);
					
				} else if(localTime.compareTo(Activities.ACTIVITY_TIMMINGS.BEFORE_08_AM) < 1) {
					return KeyValue.pair(Activities.ACTIVITY_TIMMINGS_NAMES.BEFORE_08_AM, v);
				
				} else if(localTime.compareTo(Activities.ACTIVITY_TIMMINGS.BEFORE_12_PM) < 1) {
					return KeyValue.pair(Activities.ACTIVITY_TIMMINGS_NAMES.BEFORE_12_PM, v);
				
				} else if(localTime.compareTo(Activities.ACTIVITY_TIMMINGS.BEFORE_04_PM) < 1) {
					return KeyValue.pair(Activities.ACTIVITY_TIMMINGS_NAMES.BEFORE_04_PM, v);
				
				} else if(localTime.compareTo(Activities.ACTIVITY_TIMMINGS.BEFORE_08_PM) < 1) {
					return KeyValue.pair(Activities.ACTIVITY_TIMMINGS_NAMES.BEFORE_08_PM, v);
				
				} else {
					return KeyValue.pair(Activities.ACTIVITY_TIMMINGS_NAMES.BEFORE_MIDNIGHT, v);
				
				}
			})
			.groupByKey()
			.count();
	peakUserActivityRange.toStream().to("peak-user-activity-range");	
		
		return streamsBuilder.build();
	}

// TODO: mostCommentedOnPost.
	
}
