package com.pthore.service.kafkaStreams.models;

import java.time.LocalTime;

public interface Activities {
	
	public final String ACTIVITY_TIMESTAMP = "activity_timestamp";
	
	interface ACTIVITY_TIMMINGS {
		
		public final LocalTime BEFORE_04_AM = LocalTime.of(4, 0);
		public final LocalTime BEFORE_08_AM = LocalTime.of(8, 0);
		public final LocalTime BEFORE_12_PM = LocalTime.of(12, 0);
		public final LocalTime BEFORE_04_PM = LocalTime.of(16, 0);
		public final LocalTime BEFORE_08_PM = LocalTime.of(20, 0);
		public final LocalTime BEFORE_MIDNIGHT = LocalTime.of(23, 59);

	}
	interface ACTIVITY_TIMMINGS_NAMES {
		
		public final String BEFORE_04_AM = "BEFORE_04_AM";
		public final String BEFORE_08_AM = "BEFORE_04_AM";
		public final String BEFORE_12_PM = "BEFORE_04_AM";
		public final String BEFORE_04_PM = "BEFORE_04_AM";
		public final String BEFORE_08_PM = "BEFORE_04_AM";
		public final String BEFORE_MIDNIGHT = "BEFORE_04_AM";

	}

	interface USER_PROFILE_REALTED {
		
		public final String FOLLOWED = "followed_user";
		public final String REGISTER_USER = "new_user";
		public final String UPDATE_PROFILE = "update_profile";
		public final String VIEW_PROFILE = "view_profile";
		public final String LOAD_PROFILE = "load_profile";	
	}
	
	interface POST_RELATED {
		
		public final String CREATE_POST  = "create_new_post";
		public final String VIEW_POST = "view_post";
		public final String LIST_POST = "get_posts";
		public final String CHOOSEN_DOMAIN = "choosen_domain";
		public final String LOAD_MORE_POST = "load_more_post";
		public final String CARD_CLICK = "card_click";
		public final String MINI_POST_CLICK = "mini-post-click";
		public final String DOMAIN_CLICK = "domain_click";
		public final String RELATED_POST_CLICK = "realated_post_click";
		public final String COMMENTED_ON_POST = "commented_on_post";
		public final String UPVOTED = "upvoted_post";
	}
	
	interface SEARCH_RELATED {
		public final String SEARCH_POST = "search_post";
	}
}
