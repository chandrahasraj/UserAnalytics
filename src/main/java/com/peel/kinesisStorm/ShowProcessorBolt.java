package com.peel.kinesisStorm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 
 * @author chandra
 *
 * The class is used for creating maps that contain show ids and viewers count and also showid and list of users.
 */
public class ShowProcessorBolt
		implements IRichBolt, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ShowProcessorBolt.class);
	public static long windowSize ;
	private static long StartTime;
	private static boolean isItFirst = true;
//	private static SlidingWindow sw;
	OutputCollector _collector;
	
	Map<String, Long> showAndviewersMap = new HashMap<>();
	HashMap<String,ArrayList<String>> uniqueUsersForAshow=new HashMap<>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector=collector;
	}

	/*private static SlidingWindow getInstance() {
		if (sw == null)
			return new SlidingWindow();
		return sw;
	}*/

/**
 * 
 * This method adds shows and its viewers to showAndviewersMap, for windowSize time 
 * and once the time is done pushes this data to sliding window. Currently we are adding
 * data to showAndviewersMap only when the showid exists in the redis running on nimbus.	
 */
	@Override
	public void execute(Tuple input) {

		String userid = (String) input.getValueByField(ShowProcessorSchema.FIELD_USERID);
		String showid = (String) input.getValueByField(ShowProcessorSchema.FIELD_SHOWID);
		String RedisData = new String();
		try {
			synchronized(RedisInstance.getInstance()){
				RedisData = RedisInstance.getInstance().get(showid);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 

		if (RedisData != null) {
			if (isItFirst) {
				isItFirst = false;
				StartTime = System.currentTimeMillis();
			}

			
			synchronized (showAndviewersMap) {
//				LOG.info("Difference:"+(ShowProcessorBolt.CurrentTime() - StartTime)+" startTime:"+StartTime+" \ncurrentTime:"+ShowProcessorBolt.CurrentTime()+" window size:"+ConfigurationParameters.getWindowSize());
				if ((ShowProcessorBolt.CurrentTime() - StartTime) <= 5000) {
					long count=0l;
					if (showAndviewersMap.get(showid) != null) 
						count = showAndviewersMap.get(showid);
					showAndviewersMap.put(showid, count + 1);
//					LOG.info("current count:" + showAndviewersMap.get(showid));
					uniqueUsersMapCreation(userid, showid);
/*					LOG.info("adding data to intial List:ShowId-->" + showid
							+ " UserId-->" + userid);*/
				} else {
					long count=0l;
					if (showAndviewersMap.get(showid) != null) 
						count = showAndviewersMap.get(showid);
					showAndviewersMap.put(showid, count + 1);
//					LOG.info("current count:" + showAndviewersMap.get(showid));
					
					uniqueUsersMapCreation(userid, showid);
/*					LOG.info("adding data to intial List:ShowId-->" + showid
							+ " UserId-->" + userid);
*/					StartTime = System.currentTimeMillis();
					HashMap<String, Long> target = new HashMap<>();
					HashMap<String, ArrayList<String>> uniqueTarget = new HashMap<>();

					HashMap<String, Long> temp = new HashMap<String, Long>(showAndviewersMap);
					HashMap<String, ArrayList<String>> uniqueTemp = new HashMap<>(uniqueUsersForAshow);
					
					temp.keySet().removeAll(target.keySet());
					uniqueTemp.keySet().removeAll(uniqueTarget.keySet());
					
					target.putAll(temp);
					uniqueTarget.putAll(uniqueTemp);
					
					if (target.size() > 0 && uniqueTarget.size() > 0) {
						emitHashMap(target, uniqueTarget);
					}
				}
			}
		}
		_collector.ack(input);
	}
	
	/**
	 * 
	 * @param userid
	 * @param showid
	 * 
	 * we are adding the showid and userid to a map which has key as showid and value
	 * as list of userids.
	 */
	private void uniqueUsersMapCreation(String userid, String showid) {
		synchronized (uniqueUsersForAshow) {
			if (uniqueUsersForAshow.containsKey(showid)) {
				ArrayList<String> usersList = uniqueUsersForAshow.get(showid);
				if (!usersList.contains(userid)) {
					usersList.add(userid);
				}
			} else {
				ArrayList<String> temp = new ArrayList<>();
				temp.add(userid);
				uniqueUsersForAshow.put(showid, temp);
			}
		}
	}

	private static long CurrentTime() {
		return System.currentTimeMillis();
	}

	/**
	 * 
	 * @param viewersMap
	 * @param uniqueTarget
	 * 
	 * The viewers map and unique users of shows map is thrown to the sliding window.
	 */
	private void emitHashMap(HashMap<String, Long> viewersMap, HashMap<String, ArrayList<String>> uniqueTarget) {
//		LOG.info("Throwing show map of size:"+viewersMap.size()+" and unique map:"+uniqueTarget.size()+" to sliding window");
		List<Object> tuple=new SlidingWindowSchema().deserialize(viewersMap, uniqueTarget);
		_collector.emit("mapFrames",tuple);
//		ShowProcessorBolt.getInstance().createSlidingWindow(viewersMap,uniqueTarget);
		synchronized (showAndviewersMap) {
			showAndviewersMap.clear();
			uniqueUsersForAshow.clear();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("mapFrames", new SlidingWindowSchema().getOutputFields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
