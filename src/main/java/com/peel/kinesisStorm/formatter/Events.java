package com.peel.kinesisStorm.formatter;

public class Events {

	String user_id;
	Data[] events;
	String ip;

	
	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public Data[] getEvents() {
		return events;
	}

	public void setEvents(Data[] events) {
		this.events = events;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

}
