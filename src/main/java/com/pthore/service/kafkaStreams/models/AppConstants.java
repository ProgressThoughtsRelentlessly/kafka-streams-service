package com.pthore.service.kafkaStreams.models;

public interface AppConstants {
	
	public interface KAFKA_STREAMS {
		public final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
		public final String APPLICATION_ID = "user_activity_tracker";
		public final String INPUT_TOPIC = "users_activities";
	}
}
