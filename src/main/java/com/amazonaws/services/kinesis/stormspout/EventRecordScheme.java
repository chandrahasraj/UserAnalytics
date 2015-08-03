package com.amazonaws.services.kinesis.stormspout;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * 
 * @author chandra
 * 
 */

/*
 * scheme for emiting event ids from the parsing bolt to the memcache bolt
 */
public class EventRecordScheme {
	/**
	 * Name of the (eventid)value in the record of the tuple
	 */
	public static final String FIELD_PARITIONKEY = "paritionKey";
	/**
	 * Name of the (Kinesis record) value in the tuple.
	 */
	public static final String FIELD_USERS_LIST = "usersList";
	
	public static final String FIELD_NOGROUP="nogroup";


	public List<Object> deserialize(String paritionKey, List<Object> users)
			throws JsonProcessingException {
		final List<Object> l = new ArrayList<>();
		l.add(paritionKey);
		l.add(users);
		l.add("nogroup");
		return l;
	}

	public Fields getOutputFields() {
		return new Fields(FIELD_PARITIONKEY, FIELD_USERS_LIST,FIELD_NOGROUP);
	}

}
