package world.we.deserve;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/*
*@author Miguel Ángel Fernández Fernández
*
*/
public class BasicTopology {

	static Logger log = LogManager.getLogger(BasicTopology.class.getName());

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("randominteger-spout", new BasicSpout(), 1);
		builder.setBolt("multiply", new MultiplyValueBolt(),1).shuffleGrouping("randominteger-spout");
		builder.setBolt("printer", new BasicPrinterBolt(),3).shuffleGrouping("multiply"); ;
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
				
		cluster.submitTopology("basic-test", conf, builder.createTopology());
	}
}
