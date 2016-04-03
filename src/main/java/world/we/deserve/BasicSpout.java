package world.we.deserve;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
*@author Miguel Ángel Fernández Fernández
*
* Basic bolt that generates a integer every second
*/
public class BasicSpout extends BaseRichSpout {

	private int max, min;
	private SpoutOutputCollector collector;
	Random random;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		random = new Random();
		max= 10;
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("randomint"));
	}

	public void nextTuple() {
		
		  // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = random.nextInt(max +1 );

	    
		collector.emit(new Values(randomNum));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}

	}
}
