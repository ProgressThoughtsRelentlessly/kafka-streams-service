package com.pthore.service.kafkaStreams.models;

import java.time.LocalDateTime;

public class UserActivity {
	
	private String userEmail;
	private String endpointExplored;
	private boolean viewedPost;
	private String domain;
	private String postId;
	private String postTitle;
	private String activity;
	private boolean isLoggedInUser;
	private String ipAddress;
	private String timespent;
	private String searchSentence;
	private LocalDateTime activityTimestamp;
	
	
	public LocalDateTime getActivityTimestamp() {
		return activityTimestamp;
	}
	public void setActivityTimestamp(LocalDateTime activityTimestamp) {
		this.activityTimestamp = activityTimestamp;
	}
	public String getSearchSentence() {
		return searchSentence;
	}
	public void setSearchSentence(String searchSentence) {
		this.searchSentence = searchSentence;
	}
	public String getUserEmail() {
		return userEmail;
	}
	public void setUserEmail(String userEmail) {
		this.userEmail = userEmail;
	}
	public String getPostId() {
		return postId;
	}
	public void setPostId(String postId) {
		this.postId = postId;
	}
	public String getEndpointExplored() {
		return endpointExplored;
	}
	public void setEndpointExplored(String endpointExplored) {
		this.endpointExplored = endpointExplored;
	}
	public boolean isViewedPost() {
		return viewedPost;
	}
	public void setViewedPost(boolean viewedPost) {
		this.viewedPost = viewedPost;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public String getPostTitle() {
		return postTitle;
	}
	public void setPostTitle(String postTitle) {
		this.postTitle = postTitle;
	}
	public String getActivity() {
		return activity;
	}
	public void setActivity(String activity) {
		this.activity = activity;
	}
	public boolean isLoggedInUser() {
		return isLoggedInUser;
	}
	public void setLoggedInUser(boolean isLoggedInUser) {
		this.isLoggedInUser = isLoggedInUser;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getTimespent() {
		return timespent;
	}
	public void setTimespent(String timespent) {
		this.timespent = timespent;
	}
	
	
	
	
}
