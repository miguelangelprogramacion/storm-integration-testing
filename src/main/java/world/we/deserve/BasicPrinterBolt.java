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

/*
*@author Miguel Ángel Fernández Fernández
*
*/
public class BasicPrinterBolt extends BaseRichBolt {

	OutputCollector _collector;
	static Logger logg = LogManager.getLogger(BasicPrinterBolt.class.getName());
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;		
	}

	public void execute(Tuple tuple) {
				
		System.out.println(tuple.getInteger(0)+" multiply "+tuple.getInteger(1));
		
		_collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("printedvalue"));
	}
}
