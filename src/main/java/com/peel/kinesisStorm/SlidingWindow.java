package com.peel.kinesisStorm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.core.JsonProcessingException;

public class SlidingWindow implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(SlidingWindow.class);
	private static Map<String, Long> FinalMapOfShows = new HashMap<>();
	
	static boolean firstTime = true;
	private static CircularFifoBuffer timerQueue = new CircularFifoBuffer(1);
	private static CircularFifoBuffer uniqueViewersQueue=new CircularFifoBuffer(120);
	private static CircularFifoBuffer showAndviewersQueue =new CircularFifoBuffer(120);
//	private static String serviceAddress = "http://localhost:8080/analytics/topshowsLocal";
	private static String serviceAddress = "http://ec2-54-235-81-22.compute-1.amazonaws.com:8080/analytics/topchannels";
	private static long totalUniqueViewersCount=0l;
	private static long displayCount=0l;

	public void createSlidingWindow(HashMap<String, Long> intialList2, HashMap<String, ArrayList<String>> uniqueTarget) {
//		ConfigurationParameters.getUniqueviewersqueue().add(uniqueTarget);
		uniqueViewersQueue.add(uniqueTarget);
		mainExecuter(intialList2);
	}
/**
 * 
 * @param list
 * @param usersList
 * 
 * A queue is maintained for maps in such a way that everytime we receive new data the old will be released. 
 * Currently we have kept the size of the queue as 6 
 * example:
 * data is entering with key as showid and value as no of viewers. after 10 sec the map is full and thrown then we add it to queue.
 *  9:00:10--queue||||||map1|-->9:00:20--queue|||||map1|map2|-->9:00:30--queue||||map1|map2|map3|-->
 *  9:00:40--queue|||map1|map2|map3|map4|-->9:00:50--queue||map1|map2|map3|map4|map5|-->
 *  9:01:00--queue|map1|map2|map3|map4|map5|map6|
 *  now the queue is full  and we have data from 9:00:10 to 9:01:00
 *   9:01:10--queue|map2|map3|map4|map5|map6|map7|, as you can see the first map is removed.
 *   now we have data from 9:00:20 to 9:01:10 so we have changed data after every 10 sec.
 */
	
	
	private void mainExecuter(HashMap<String, Long> list) {
//		checkForDatalessTimeFrames();
		if (firstTime) {
			if (!showAndviewersQueue.isFull()) {
				addMapToSlidingMap(list);
				showAndviewersQueue.add(list);
			} else {
				LOG.info("Buffer is full for the first time, re-aligning and creating Json");
				realigningFinalMap(list);
				firstTime = false;
			}
		} else {
			LOG.info("creating Json from final Map");
			realigningFinalMap(list);
		}
		createJson();
	}

	
	/**
	 * There could be a lot of gaps in the flow of data where in we receive no data or error data
	 * because of which we wouldn't be having data that is exactly with the sliding window time. 
	 * example:
	 * 9:00:00 Queue|||||map1| --> 9:00:10 Queue||||map1|map2| after map2 data wasn't available till 9:00:30
	 * so we calculate the gap occured using this method and fill the gaps with empty maps to ensure 
	 * the data is synchronous with time.
	 */
	@SuppressWarnings("unchecked")
	private void checkForDatalessTimeFrames() {
		if (!timerQueue.isEmpty()) {
			long lastInsertedTime = (Long) timerQueue.get();
			long CurrentTime = System.currentTimeMillis();
			if (lastInsertedTime != 0l&& CurrentTime - lastInsertedTime > ShowProcessorBolt.windowSize) {
				int numberOfEmptyMaps = (int) ((CurrentTime - lastInsertedTime) / ShowProcessorBolt.windowSize);
				LOG.info("number of empty maps to add:"+(CurrentTime - lastInsertedTime) / ShowProcessorBolt.windowSize);
				
				for (int i = 0; i < numberOfEmptyMaps; i++) {
					HashMap<String,Long> leftOutFromShowMap=(HashMap<String, Long>) showAndviewersQueue.get();
					showAndviewersQueue.add(new HashMap<String, Long>());
					uniqueViewersQueue.add(new HashMap<String, ArrayList<String>>());
				
					if(!leftOutFromShowMap.isEmpty()&&showAndviewersQueue.isFull()){
						removeMapFromSlidingMap(leftOutFromShowMap);
					}
				}
			}
		}
		timerQueue.add(System.currentTimeMillis());
	}

	/**
	 * This method is used for creating json 
	 */
	private void createJson() {
		HashMap<String, Long> target = new HashMap<>();
		HashMap<String, Long> temp = new HashMap<String, Long>(FinalMapOfShows);

		temp.keySet().removeAll(target.keySet());
		target.putAll(temp);
		
//		LinkedHashMap<String,String> temp2=(LinkedHashMap<String, String>) sortByComparator(target);
		LinkedHashMap<String, String> PasserMap=displayUniqueUsers(target);
		
		String anotherJsonString=generateValidJson(PasserMap);
		if(displayCount%10==0)
			LOG.info(anotherJsonString);
		displayCount++;
		callDisplayService(anotherJsonString);
		
		
	}

	/**
	 * 
	 * @param orderedMapofShows
	 * @return
	 * 
	 * This method takes in the ordered map which contains showid as key and delimited string that contains
	 * no of viewers and unique users. we fetch the show image and show title from the redis on nimbus
	 * using the showid as the key.
	 */
	private String generateValidJson(Map<String, String> orderedMapofShows) {
		JsonObject jo=null;
		JsonArrayBuilder jArray=Json.createArrayBuilder();
		HashMap<String,String> dupplicateShows=new HashMap<>();
		
		for(Map.Entry<String, String> ms:orderedMapofShows.entrySet()){
			String delimitedValuesArray[] = ms.getValue().split("\\|");
			
			String showId=ms.getKey();

			String noOfViewers = delimitedValuesArray[1];
			String uniqueusers = delimitedValuesArray[0];

			String showtitle = getshowtitle(ms.getKey());
			String showimage = getshowimage(ms.getKey());
			
			if(!dupplicateShows.containsKey(showtitle)){
				dupplicateShows.put(showtitle, showId);
			}

			if (dupplicateShows.get(showtitle).equalsIgnoreCase(showId)) {
				JsonObjectBuilder flow = Json.createObjectBuilder();
				flow.add("showId", showId).add("showTitle", showtitle)
						.add("imageUrl", showimage)
						.add("noOfViewers", noOfViewers)
						.add("UniqueUsers", uniqueusers)
						.add("TotalUniqueCount",totalUniqueViewersCount)
						.add("Time", System.currentTimeMillis());
				jArray.add(flow);
			}
		}
		
		jo=Json.createObjectBuilder().add("channels",jArray).build();
		return jo.toString();
	}
	
	private String getshowimage(String key) {
		synchronized (RedisInstance.getInstance()) {
			String showdetails = RedisInstance.getInstance().get(key);
			if (showdetails != null && showdetails.split("\\|")[1] != null)
				return showdetails.split("\\|")[1];
			else
				return null;
		}
	}

	private String getshowtitle(String key) {
		synchronized (RedisInstance.getInstance()) {
			String showdetails = RedisInstance.getInstance().get(key);
			if (showdetails != null && showdetails.split("\\|")[0] != null)
				return showdetails.split("\\|")[0];
			else
				return "NOT AVAILABLE";
		}
	}

	/**
	 * 
	 * @param target
	 * @return
	 * 
	 * This method is used for calculating unique users. For demo purposes we have used multiple loops
	 * we have a uniqueviewersmap which contains maps of showids and uniqueusers (as list).
	 * we rotate the queue for all maps and combine the lists which have similar keys among the maps,
	 * and store the new keys with combined lists(if occured) into another map.later we loop through this
	 * map and add the key as showid and value as the size of arraylist from the before map.This size is the count
	 * of unique users for that key. Then we fetch only the showids which have high no of viewers from the 
	 * ordered map and compare with the unique users map and generate a map of show id as key and 
	 * a delimited string as value. 
	 */
	@SuppressWarnings("unchecked")
	private LinkedHashMap<String,String> displayUniqueUsers(HashMap<String, Long> target) {
		LinkedHashMap<String,String> finalServicePasserMap=new LinkedHashMap<>();
//		CircularFifoBuffer uniqueTemporaryBuffer = ConfigurationParameters.getUniqueTemporaryBuffer();
		CircularFifoBuffer uniqueTemporaryBuffer=new CircularFifoBuffer(120);
		ConcurrentHashMap<String,Long> tempStore=new ConcurrentHashMap<>();
		HashMap<String,ArrayList<String>> container=new HashMap<>();
		
		while (!uniqueViewersQueue.isEmpty()) {
			HashMap<String, ArrayList<String>> uniqueTempMap = (HashMap<String, ArrayList<String>>) uniqueViewersQueue.remove();
			uniqueTemporaryBuffer.add(uniqueTempMap);
			for (Map.Entry<String, ArrayList<String>> looper : uniqueTempMap
					.entrySet()) {

				if (!container.containsKey(looper.getKey())) {
					container.put(looper.getKey(), looper.getValue());
				} else {
					ArrayList<String> CombinedList = container.remove(looper.getKey());
					ArrayList<String> listTwo = looper.getValue();
					Set<String> set = new HashSet<>(CombinedList);
					set.addAll(listTwo);
					ArrayList<String> mergeList = new ArrayList<>(set);
					container.put(looper.getKey(), mergeList);
				}
			}
		}
		for (Entry<String, ArrayList<String>> looper : container.entrySet()) {
			tempStore.put(looper.getKey(), Long.valueOf(looper.getValue().size()));
				totalUniqueViewersCount+=Long.valueOf(looper.getValue().size());
		}
		
		LinkedHashMap<String,Long> orderedMap=(LinkedHashMap<String, Long>) sortByComparator(tempStore);
		
		for (Map.Entry<String, Long> looper : orderedMap.entrySet()) {
			if (target.containsKey(looper.getKey())) {
				String temp ="";
				temp += looper.getValue()+"|" + target.get(looper.getKey());
				/*System.out.println("key:" + looper.getKey() + " value:"
						+ tempStore.get(looper.getKey()));*/
				finalServicePasserMap.put(looper.getKey(), temp);
			}
		}
		while (!uniqueTemporaryBuffer.isEmpty()) {
			uniqueViewersQueue.add(uniqueTemporaryBuffer.remove());
		}
		return finalServicePasserMap;
	}
	
	/**
	 * 
	 * @param postParameter
	 * 
	 * This method calls the service with json as a post parameter.
	 */
	private void callDisplayService(String postParameter) {
		totalUniqueViewersCount=0l;
		try {
			HttpClient client = HttpClientBuilder.create().build();
			HttpPost post = new HttpPost(serviceAddress);
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
			nameValuePairs.add(new BasicNameValuePair("event", postParameter)); 
			post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
			client.execute(post);
		} catch (JsonProcessingException e) {
			LOG.error("Json processing exception,illegal data:"+e);
		} catch (IOException e) {
			LOG.error("Connection Problem, unable to throw data to test server:"+e);
		}
	}


	/**
	 * 
	 * @param list
	 * 
	 * Here we pass showsList(latest) which contains key as showId and value as no of viewers.
	 * we get the first inserted map from the showqueue and remove its elements from the end map 
	 * and add the new showList to the final map.
	 */
	private void realigningFinalMap(HashMap<String, Long> list) {
		@SuppressWarnings("unchecked")
		HashMap<String, Long> lastMap = (HashMap<String, Long>) showAndviewersQueue.get();
		showAndviewersQueue.add(list);
		removeMapFromSlidingMap(lastMap);
		addMapToSlidingMap(list);
	}

	private void removeMapFromSlidingMap(HashMap<String, Long> lastMap) {
		for (Map.Entry<String, Long> mp : lastMap.entrySet()) {
			long countInFinalMap = 0l;
			if (FinalMapOfShows.containsKey(mp.getKey()))
				countInFinalMap = FinalMapOfShows.remove(mp.getKey());
			long delta = countInFinalMap > mp.getValue() ? countInFinalMap
					- mp.getValue() : mp.getValue() - countInFinalMap;
//					if(delta>0)
						FinalMapOfShows.put(mp.getKey(), delta);
		}
	}

	/**
	 * 
	 * @param list
	 * 
	 *  Here we pass showList which contains key as showId and value as no of viewers.
	 * we add this data to the end map.
	 */
	private void addMapToSlidingMap(HashMap<String, Long> list) {
		for (Map.Entry<String, Long> mp : list.entrySet()) {
			long countInFinalMap = 0l;
			if (FinalMapOfShows.containsKey(mp.getKey()))
				countInFinalMap = FinalMapOfShows.remove(mp.getKey());
			FinalMapOfShows.put(mp.getKey(), countInFinalMap + mp.getValue());
		}
	}

	/**
	 * 
	 * @param unsortMap
	 * @return
	 * 
	 * comparator used for ordering show viewers. the order is done on the values in 
	 * descending order.
	 */
	private static Map<String, Long> sortByComparator(Map<String, Long> unsortMap) {
		// Convert Map to List
//		String RedisData = new String();
//		String imageUrl;
		List<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String, Long>>(
				unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1,
					Map.Entry<String, Long> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		// Convert sorted map back to a Map
		int displayCounter = 15;
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		try {
			for (Iterator<Map.Entry<String, Long>> it = list.iterator(); it.hasNext();) {
				Map.Entry<String, Long> entry = it.next();
				sortedMap.put(entry.getKey(),entry.getValue());
				
				if (displayCounter == 0)
					break;
				displayCounter--;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return sortedMap;
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
	}
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		HashMap<String, Long> showAndviewersMap=(HashMap<String, Long>)input.getValueByField(SlidingWindowSchema.FIELD_SHOW_MAP);
		HashMap<String, ArrayList<String>> uniqueViewersMap=(HashMap<String, ArrayList<String>>)input.getValueByField(SlidingWindowSchema.FIELD_VIWERS_MAP);
//		LOG.info("received Size of maps, show map:"+showAndviewersMap.size()+" and uniquemap:"+uniqueViewersMap.size());
		new SlidingWindow().createSlidingWindow(showAndviewersMap, uniqueViewersMap);
	}
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
