package com.peel.kinesisStorm.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PeelDataFormat {
	String serverTime, userId, sessionId, roomId, contextId, screenId,
			widgetId, eventId, cTimeStamp, charParam1, charParam2, charParam3,
			charParam4, country;
	int intParam1, intParam2, intParam3, intParam4;

	/**
	 * @param serverTime
	 * @param userId
	 * @param sessionId
	 * @param roomId
	 * @param contextId
	 * @param screenId
	 * @param widgetId
	 * @param eventId
	 * @param cTimeStamp
	 * @param charParam1
	 * @param intParam1
	 * @param charParam2
	 * @param intParam2
	 * @param charParam3
	 * @param intParam3
	 * @param charParam4
	 * @param intParam4
	 * @throws Exception
	 */
	public PeelDataFormat(String serverTime, String userId, String sessionId,
			String roomId, String contextId, String screenId, String widgetId,
			String eventId, String cTimeStamp, String charParam1,
			int intParam1, String charParam2, int intParam2, String charParam3,
			int intParam3, String charParam4, int intParam4) {
		super();
		this.serverTime = serverTime;
		this.userId = userId;
		this.sessionId = sessionId;
		this.roomId = roomId;
		this.contextId = contextId;
		this.screenId = screenId;
		this.widgetId = widgetId;
		this.eventId = eventId;
		this.cTimeStamp = cTimeStamp;
		this.charParam1 = charParam1;
		this.intParam1 = intParam1;
		this.charParam2 = charParam2;
		this.intParam2 = intParam2;
		this.charParam3 = charParam3;
		this.intParam3 = intParam3;
		this.charParam4 = charParam4;
		this.intParam4 = intParam4;
	}

	/**
	 * 
	 * @param serverTime
	 * @param userId
	 * @param sessionId
	 * @param roomId
	 * @param contextId
	 * @param screenId
	 * @param widgetId
	 * @param eventId
	 * @param cTimeStamp
	 * @param charParam1
	 * @param intParam1
	 * @param charParam2
	 * @param intParam2
	 * @param charParam3
	 * @param intParam3
	 * @param charParam4
	 * @param intParam4
	 * @param addCountryParam
	 */
	public PeelDataFormat(String serverTime, String userId, String sessionId,
			String roomId, String contextId, String screenId, String widgetId,
			String eventId, String cTimeStamp, String charParam1,
			int intParam1, String charParam2, int intParam2, String charParam3,
			int intParam3, String charParam4, int intParam4,
			boolean addCountryParam) {
		this(serverTime, userId, sessionId, roomId, contextId, screenId,
				widgetId, eventId, cTimeStamp, charParam1, intParam1,
				charParam2, intParam2, charParam3, intParam3, charParam4,
				intParam4);
		/*if (addCountryParam) {
			String Country = RedisInstance.getInstance().get(this.charParam3);
			if(country!=null)
				this.country = Country;
			else
				this.country="";
		}*/
		this.country="";
	}

	@Override
	public String toString() {
		try {
			return new ObjectMapper().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return super.toString();
		}
	}

	public String getServerTime() {
		return serverTime;
	}

	public void setServerTime(String serverTime) {
		this.serverTime = serverTime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getRoomId() {
		return roomId;
	}

	public void setRoomId(String roomId) {
		this.roomId = roomId;
	}

	public String getContextId() {
		return contextId;
	}

	public void setContextId(String contextId) {
		this.contextId = contextId;
	}

	public String getScreenId() {
		return screenId;
	}

	public void setScreenId(String screenId) {
		this.screenId = screenId;
	}

	public String getWidgetId() {
		return widgetId;
	}

	public void setWidgetId(String widgetId) {
		this.widgetId = widgetId;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getcTimeStamp() {
		return cTimeStamp;
	}

	public void setcTimeStamp(String cTimeStamp) {
		this.cTimeStamp = cTimeStamp;
	}

	public String getCharParam1() {
		return charParam1;
	}

	public void setCharParam1(String charParam1) {
		this.charParam1 = charParam1;
	}

	public int getIntParam1() {
		return intParam1;
	}

	public void setIntParam1(int intParam1) {
		this.intParam1 = intParam1;
	}

	public String getCharParam2() {
		return charParam2;
	}

	public void setCharParam2(String charParam2) {
		this.charParam2 = charParam2;
	}

	public int getIntParam2() {
		return intParam2;
	}

	public void setIntParam2(int intParam2) {
		this.intParam2 = intParam2;
	}

	public String getCharParam3() {
		return charParam3;
	}

	public void setCharParam3(String charParam3) {
		this.charParam3 = charParam3;
	}

	public int getIntParam3() {
		return intParam3;
	}

	public void setIntParam3(int intParam3) {
		this.intParam3 = intParam3;
	}

	public String getCharParam4() {
		return charParam4;
	}

	public void setCharParam4(String charParam4) {
		this.charParam4 = charParam4;
	}

	public int getIntParam4() {
		return intParam4;
	}

	public void setIntParam4(int intParam4) {
		this.intParam4 = intParam4;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

}
