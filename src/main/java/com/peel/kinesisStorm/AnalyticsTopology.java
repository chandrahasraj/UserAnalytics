/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.peel.kinesisStorm;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.stormspout.DefaultKinesisRecordScheme;
import com.amazonaws.services.kinesis.stormspout.EventRecordScheme;
import com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme;
import com.amazonaws.services.kinesis.stormspout.KinesisSpout;
import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;

/**
 * 
 * @author chandra
 *
 *This is the main class that handles the complete topology.
 */
public class AnalyticsTopology extends StormConfigurator {
	
	
	public AnalyticsTopology(String propertiesFile) {
		super(propertiesFile);
	}

	private static Logger LOG = LoggerFactory.getLogger(AnalyticsTopology.class);

	public static void main(String[] args) throws IllegalArgumentException,
			KeeperException, InterruptedException, AlreadyAliveException,
			InvalidTopologyException, IOException {

		String propertiesFile = null;
		IKinesisRecordScheme scheme = new DefaultKinesisRecordScheme();
		String mode = null;

		//releasing cache of ip's for every 30 sec 
		java.security.Security.setProperty("networkaddress.cache.ttl" , "30");
		
		if (args.length != 2) {
			printUsageAndExit();
		} else {
			propertiesFile = args[0];
			mode = args[1];
		}

		
		StormConfigurator config = new AnalyticsTopology(propertiesFile);
		new ConfigurationParameters(config);

		
		final KinesisSpoutConfig SpoutConfig = new KinesisSpoutConfig(
				config.STREAM_NAME, config.MAX_RECORDS,
				config.INTIAL_POSITION_IN_STREAM, config.ZOOKEEPER_PREFIX,
				config.ZOOKEEPER_ENDPOINT, config.ZOOKEEPER_SESSIONTIMEOUT,
				config.CHECKPOINT_INTERVAL, scheme);

		final KinesisSpout spout = new KinesisSpout(SpoutConfig,new CustomCredentialsProviderChain(), new ClientConfiguration());
		
		TopologyBuilder builder = new TopologyBuilder();

		// Using number of shards as the parallelism hint for the spout.
		builder.setSpout("kinesis_spout", spout, 2);
		builder.setBolt("print_bolt", new ParsingBolt(), 2).fieldsGrouping("kinesis_spout",new Fields(DefaultKinesisRecordScheme.FIELD_PARTITION_KEY));
		builder.setBolt("topchannels", new ParameterProcessorBolt(), 2).fieldsGrouping("print_bolt", "processing",new Fields(EventRecordScheme.FIELD_NOGROUP));
		builder.setBolt("calculateTopChannels", new ShowProcessorBolt(), 1).fieldsGrouping("topchannels", "channelslist",
						new Fields(ShowProcessorSchema.FIELD_NOGROUPING));
		builder.setBolt("slidingWindow",new SlidingWindow(),1).fieldsGrouping("calculateTopChannels","mapFrames",new Fields(SlidingWindowSchema.FIELD_NOGROUP));

		Config topoConf = new Config();
		topoConf.setFallBackOnJavaSerialization(true);
		topoConf.setDebug(false);

		if (mode.equals("LocalMode")) {
			LOG.info("Starting sample storm topology in LocalMode ...");
			new LocalCluster().submitTopology("test_spout", topoConf,builder.createTopology());
		} else if (mode.equals("RemoteMode")) {
			topoConf.setNumWorkers(1);
			topoConf.setMaxSpoutPending(5000);
			LOG.info("Submitting sample topology " + config.TOPOLOGY_NAME+ " to remote cluster.");
			StormSubmitter.submitTopology(config.TOPOLOGY_NAME, topoConf,builder.createTopology());
		} else {
			printUsageAndExit();
		}

	}
	private static void printUsageAndExit() {
		System.out.println("Usage: " + AnalyticsTopology.class.getName()+ " <propertiesFile> <LocalMode or RemoteMode>");
		System.exit(-1);
	}
}
