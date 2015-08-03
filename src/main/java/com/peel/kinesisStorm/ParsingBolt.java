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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.DefaultKinesisRecordScheme;
import com.amazonaws.services.kinesis.stormspout.EventRecordScheme;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.peel.kinesisStorm.formatter.JsonFormatter;

public class ParsingBolt implements IRichBolt {
    private static final long serialVersionUID = 177788290277634253L;
    private static final Logger LOG = LoggerFactory.getLogger(ParsingBolt.class);
    private static final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private OutputCollector _collector;
    
    public void execute(Tuple input, BasicOutputCollector collector) {
        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream("processing",new EventRecordScheme().getOutputFields());
    }

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector=collector;
	}

	@Override
	public void execute(Tuple input) {
		Record record = (Record)input.getValueByField(DefaultKinesisRecordScheme.FIELD_RECORD);
        ByteBuffer buffer = record.getData();
        String data = null; 
        String partitionKey=record.getPartitionKey();
        List<Object> FormattedData=null;
        List<Object> tuple=null;
		synchronized (decoder) {
			try {
				data = decoder.decode(buffer).toString();
				FormattedData = new JsonFormatter().alternateParsingData(data);
				tuple = new EventRecordScheme().deserialize(partitionKey,
						FormattedData);
			} catch (CharacterCodingException e) {
				LOG.error("Exception when decoding record ", e);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        if(FormattedData!=null){
        	_collector.emit("processing",tuple);
        }
        _collector.ack(input);
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
