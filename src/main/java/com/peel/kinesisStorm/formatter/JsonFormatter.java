package com.peel.kinesisStorm.formatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class JsonFormatter {

	/**
	 * @author chandra
	 */

	private static Log LOG = LogFactory.getLog(JsonFormatter.class);

	@SuppressWarnings("unchecked")
	public List<Object> alternateParsingData(String record)
			throws JsonParseException, JsonMappingException, IOException {
		List<Object> users = new ArrayList<>();
		List<Object> errorData = new ArrayList<>();

		Object oj = null;
		try {
			oj = new JSONParser().parse(record.toString());
		} catch (ParseException e1) {
			LOG.error("Parser Exception, the record has redundant format throwing the redundant data");
			return null;
		}
		JSONObject ob = (JSONObject) oj;
		JSONArray arr = (JSONArray) ob.get("events");
		Iterator<JSONObject> pq = arr.iterator();
		Iterator<JSONObject> errorLooper = arr.iterator();

		while (errorLooper.hasNext()) {
			errorData.add((String) errorLooper.next().get("data"));
		}

		try {
			while (pq.hasNext()) {
				String currentData = (String) pq.next().get("data");
				users.add(this.generatePeelDataFormat(currentData.toString()));
			}
			return users;
		} catch (NumberFormatException p) {
			LOG.error("Some parameter is missing in data:"+p);
			return null;
		} catch (Exception e) {
			LOG.error(e);
			return null;
		}
	}

	private PeelDataFormat generatePeelDataFormat(String record)
			throws Exception {
		String temp = (String) record;
		String delim = String.valueOf("|");
		String esc = "\\";
		String regex = "(?<!" + Pattern.quote(esc) + ")" + Pattern.quote(delim);
		String tempFormat[] = temp.split(regex);
		PeelDataFormat user;
		String serverTime = "", userId = "", sessionId = "", roomId = "", contextId = "", screenId = "", widgetId = "", eventId = "", cTimeStamp = "", charParam1 = "", charParam2 = "", charParam3 = "", charParam4 = "";
		int intParam1 = 0, intParam2 = 0, intParam3 = 0, intParam4 = 0;
		boolean breakLoop = false;

		int i = 0;
		while (!breakLoop) {
			switch (i) {
			case 0:
				serverTime = tempFormat[i++];
				break;
			case 1:
				userId = tempFormat[i++];
				break;
			case 2:
				sessionId = tempFormat[i++];
				break;
			case 3:
				roomId = tempFormat[i++];
				break;
			case 4:
				contextId = tempFormat[i++];
				break;
			case 5:
				screenId = tempFormat[i++];
				break;
			case 6:
				widgetId = tempFormat[i++];
				break;
			case 7:
				eventId = tempFormat[i++];
				break;
			case 8:
				cTimeStamp = tempFormat[i++];
				break;
			case 9:
				try {
					charParam1 = tempFormat[i++];
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 10:
				try {
					intParam1 = Integer.parseInt(tempFormat[i++]);
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 11:
				try {
					charParam2 = tempFormat[i++];
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 12:
				try {
					intParam2 = Integer.parseInt(tempFormat[i++]);
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 13:
				try {
					charParam3 = tempFormat[i++];
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 14:
				try {
					intParam3 = Integer.parseInt(tempFormat[i++]);
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 15:
				try {
					charParam4 = tempFormat[i++];
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			case 16:
				try {
					intParam4 = Integer.parseInt(tempFormat[i++]);
					breakLoop = true;
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					breakLoop = true;
					break;
				}
			}
		}

		user = new PeelDataFormat(serverTime, userId, sessionId, roomId,
				contextId, screenId, widgetId, eventId, cTimeStamp, charParam1,
				intParam1, charParam2, intParam2, charParam3, intParam3,
				charParam4, intParam4, true);
		return user;
	}
}
