package com.peel.kinesisStorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.amazonaws.services.kinesis.stormspout.EventRecordScheme;
import com.peel.kinesisStorm.formatter.PeelDataFormat;

/**
 * 
 * @author chandra
 *
 * This class is used for sending necessary resources to the @ShowProcessorBolt class 
 * 
 */
public class ParameterProcessorBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7412387465542446584L;
//	private static final Logger LOG = LoggerFactory.getLogger(ParameterProcessorBolt.class);
	private OutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		List<Object> tuple = null;
		List<PeelDataFormat> temp = new ArrayList<>();
		synchronized (temp) {
			temp = (List<PeelDataFormat>) input
					.getValueByField(EventRecordScheme.FIELD_USERS_LIST);

			for (int listLength = 0;listLength < temp.size();listLength++) {
				if (Long.valueOf(temp.get(listLength).getEventId())==1011l) {
					tuple = new ShowProcessorSchema().deserialize(temp.get(listLength).getCharParam3(), 1l,temp.get(listLength).getCountry()
							,temp.get(listLength).getUserId(),temp.get(listLength).getCharParam2());
					_collector.emit("channelslist", tuple);
				}
			}
			_collector.ack(input);
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	/**
	 * Here we declare a stream. This stream expects the fields we mentioned in the 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("channelslist",
				new ShowProcessorSchema().getOutputFields());
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		    return null;
	}
}
