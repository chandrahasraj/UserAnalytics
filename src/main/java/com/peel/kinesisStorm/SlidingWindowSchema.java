package com.peel.kinesisStorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;

public class SlidingWindowSchema {

	public static final String FIELD_NOGROUP = "NoGROUP";
	public static final String FIELD_VIWERS_MAP="viewersMap";
	public static final String FIELD_SHOW_MAP="showMap";
	
	public List<Object> deserialize(HashMap<String, Long> viewersMap, HashMap<String, ArrayList<String>> uniqueTarget) {
		final List<Object> l = new ArrayList<>();
		l.add(viewersMap);
		l.add(uniqueTarget);
		l.add("FIELD_NOGROUPING");
		return l;
	}
	
	public Fields getOutputFields() {
		return new Fields(FIELD_SHOW_MAP, FIELD_VIWERS_MAP, FIELD_NOGROUP);
	}

}
