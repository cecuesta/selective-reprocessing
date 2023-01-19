package edu.doc_ti.jfcp.selec_reproc.storm.bolt;


import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProcessBoltDummy extends BaseRichBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 689163422885984769L;

	protected static final Logger LOG = LoggerFactory.getLogger(ProcessBoltDummy.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("DUMMY input = [" + tuple + "]");
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    
    
    
}