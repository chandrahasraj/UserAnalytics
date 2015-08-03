package com.peel.kinesisStorm;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;

/**
 * 
 * @author chandra
 *
 *	show processor schema, which is used for obtaining/throwing the fields once emitted to the stream or to be emitted to the stream
 */
public class ShowProcessorSchema {

	public static final String FIELD_CHANNELSID = "channelsId";
	public static final String FIELD_COUNTER = "channelCount";
	public static final String FIELD_COUNTRYID = "countryId";
	public static final String FIELD_NOGROUPING = "noGrouping";
	public static final String FIELD_USERID = "userId";
	public static final String FIELD_SHOWID = "showId";

	/**
	 * 
	 * @param channels
	 * @return
	 * 
	 * a list of object which is regarded as a tuple is created which has various fields.
	 */
	public List<Object> deserialize(String channelId, long viewersCount, String country,String UserId,String ShowId) {
		final List<Object> l = new ArrayList<>();
		l.add(channelId);
		l.add(viewersCount);
		l.add(country);
		l.add(UserId);
		l.add(ShowId);
		l.add("FIELD_NOGROUPING");
		return l;
	}

	/**
	 * 
	 * @return
	 * The output fields required by the stream
	 */
	public Fields getOutputFields() {
		return new Fields(FIELD_CHANNELSID, FIELD_COUNTER, FIELD_COUNTRYID,FIELD_USERID,FIELD_SHOWID,
				FIELD_NOGROUPING);
	}
}
