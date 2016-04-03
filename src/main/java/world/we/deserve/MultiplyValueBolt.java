package world.we.deserve;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
*@author Miguel Ángel Fernández Fernández
*
*/
public class MultiplyValueBolt extends BaseRichBolt {

	OutputCollector _collector;
	static Logger logg = LogManager.getLogger(BasicPrinterBolt.class.getName());
	int factor;
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		factor = 2;
	}

	public void execute(Tuple tuple) {
				
		_collector.emit(tuple, new Values(tuple.getInteger(0), tuple.getInteger(0) * factor));
		_collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("value", "multiplyvalue"));
	}
}

