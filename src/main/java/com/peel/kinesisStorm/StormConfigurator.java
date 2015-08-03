package com.peel.kinesisStorm;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;

/**
 * 
 * @author chandra
 *
 *This class is used for configuring all the required properties.
 */
public class StormConfigurator {

	private static final Log LOG=LogFactory.getLog(StormConfigurator.class);
	
	private static final String PROP_TOPOLOGY_NAME = "topologyName";
	private static final String PROP_KINESIS_STREAM_NAME = "kinesisStreamName";
	private static final String PROP_ZOOKEEPER_ENDPOINT = "zookeeperEndpoint";
	private static final String PROP_ZOOKEEPER_PREFIX = "zookeeperPrefix";
	private static final String PROP_ZOOKEEPER_SESSION_TIMEOUTINMILLIS = "ZookeeperSessionTimeoutInMillis";
	private static final String PROP_CHECKPOINT_INTERVAL_TIMEINMILLIS = "KinesisCheckpointIntervalInMillis";
	private static final String PROP_MAXRECORDS_PERCALL = "MaxRecords";
	private static final String PROP_INTIALPOISITION_IN_STREAM = "initialPositionInStream";
	private static final String PROP_SLIDINGWINDOW_FRAME_SIZE="window_frame_size_in_millis";
	private static final String PROP_SERIVE_ADDRESS="service_address";
	private static final String PROP_NO_OF_FRAMES="number_of_frames";

	private static final String DEFAULT_TOPOLOGY_NAME = "topology_name";
	private static final String DEFAULT_KINESIS_STREAM_NAME = "kinesis_stream_name";
	private static final String DEFAULT_ZOOKEEPER_ENDPOINT = "localhost:2181";
	private static final String DEFAULT_ZOOKEEPER_PREFIX = "kinesis_spout";
	private static final long DEFAULT_ZOOKEEPER_SESSION_TIMEOUTINMILLIS = 10000l;
	private static final long DEFAULT_CHECKPOINT_INTERVAL_TIMEINMILLIS = 60000l;
	private static final int DEFAULT_MAXRECORDS_PERCALL = 10000;
	private static final String DEFAULT_INTIALPOISITION_IN_STREAM = "initialPositionInStream";
	private static final long DEFAULT_SLIDINGWINDOW_FRAME_SIZE=10000l;
	private static final String DEFAULT_SERVICE_ADDRESS="http:////localhost:8080//topchannels";
	private static final int DEFAULT_NUMBEROF_FRAMES=6;

	protected final String TOPOLOGY_NAME;
	protected final String STREAM_NAME;
	protected final InitialPositionInStream INTIAL_POSITION_IN_STREAM;
	protected final String ZOOKEEPER_ENDPOINT;
	protected final String ZOOKEEPER_PREFIX;
	protected final long ZOOKEEPER_SESSIONTIMEOUT;
	protected final long CHECKPOINT_INTERVAL;
	protected final int MAX_RECORDS;
	protected final long WINDOW_FRAME_SIZE;
	protected final String SERVICE_ADDRESS;
	protected final int NUMBER_OF_FRAMES;

	public StormConfigurator(String propertiesFile) {
		Properties properties = new Properties();
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(propertiesFile);
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		TOPOLOGY_NAME = properties.getProperty(PROP_TOPOLOGY_NAME,DEFAULT_TOPOLOGY_NAME);
		LOG.info("Using topology name " + TOPOLOGY_NAME);
		
		STREAM_NAME = properties.getProperty(PROP_KINESIS_STREAM_NAME,DEFAULT_KINESIS_STREAM_NAME);
		LOG.info("Using stream name " + STREAM_NAME);
		
		INTIAL_POSITION_IN_STREAM = InitialPositionInStream.valueOf(properties.getProperty(PROP_INTIALPOISITION_IN_STREAM,DEFAULT_INTIALPOISITION_IN_STREAM));
		LOG.info("Using initial position " + INTIAL_POSITION_IN_STREAM.toString()+ " (if a checkpoint is not found).");

		ZOOKEEPER_ENDPOINT = properties.getProperty(PROP_ZOOKEEPER_ENDPOINT,DEFAULT_ZOOKEEPER_ENDPOINT);
		LOG.info("Using zookeeper endpoint " + ZOOKEEPER_ENDPOINT);
		
		ZOOKEEPER_PREFIX = properties.getProperty(PROP_ZOOKEEPER_PREFIX,DEFAULT_ZOOKEEPER_PREFIX);
		LOG.info("Using zookeeper prefix " + ZOOKEEPER_PREFIX);
		
		ZOOKEEPER_SESSIONTIMEOUT = parseLong(PROP_ZOOKEEPER_SESSION_TIMEOUTINMILLIS,DEFAULT_ZOOKEEPER_SESSION_TIMEOUTINMILLIS, properties);
		LOG.info("zookeeper session timeout in millis " + ZOOKEEPER_SESSIONTIMEOUT);
		
		CHECKPOINT_INTERVAL = parseLong(PROP_CHECKPOINT_INTERVAL_TIMEINMILLIS,DEFAULT_CHECKPOINT_INTERVAL_TIMEINMILLIS, properties);
		LOG.info("kinesis checkpoint interval in millis " + CHECKPOINT_INTERVAL);
		
		MAX_RECORDS = parseInt(PROP_MAXRECORDS_PERCALL,DEFAULT_MAXRECORDS_PERCALL, properties);
		LOG.info("max records to fetch from stream " + MAX_RECORDS);
		
		WINDOW_FRAME_SIZE=parseLong(PROP_SLIDINGWINDOW_FRAME_SIZE,DEFAULT_SLIDINGWINDOW_FRAME_SIZE,properties);
		LOG.info("sliding window frame size "+WINDOW_FRAME_SIZE);
		
		SERVICE_ADDRESS=properties.getProperty(PROP_SERIVE_ADDRESS, DEFAULT_SERVICE_ADDRESS);
		LOG.info("service address "+SERVICE_ADDRESS);
		
		NUMBER_OF_FRAMES=parseInt(PROP_NO_OF_FRAMES,DEFAULT_NUMBEROF_FRAMES,properties);
		LOG.info("Number of frames:"+NUMBER_OF_FRAMES);
	}

	private static long parseLong(String property, long defaultValue,
			Properties properties) {
		return Long.parseLong(properties.getProperty(property,
				Long.toString(defaultValue)));
	}

	private static int parseInt(String property, int defaultValue,
			Properties properties) {
		return Integer.parseInt(properties.getProperty(property,
				Integer.toString(defaultValue)));
	}
}
