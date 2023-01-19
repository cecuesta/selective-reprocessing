package edu.doc_ti.jfcp.selec_reproc.storm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.doc_ti.jfcp.selec_reproc.utils.MyESUtils;


public class ESInserterBolt extends BaseRichBolt{
	public static final Logger LOG = LoggerFactory.getLogger(ESInserterBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -3935565887934182660L;
	
	private OutputCollector _collector;

	private boolean INSERT_INTO_ES_ENABLED = true; 

	@SuppressWarnings("rawtypes")
	Map propsLookup = null ;

	int counter = 0 ;
	long counterV2 = 0 ;

    private BulkProcessor bulkProcessor = null ;
 	
//	long nanosExecInsertES = 0 ;
	long countExecNanos = 0 ;
	long tsSpeed = 0 ;
	long tsSpeedV2 = 0 ;
	
	int numTicks = 0 ;
	
//	static HashMap<String, String> startTopHM = new HashMap<String, String>();

//	private String mode = "desc";
//	
//	public ESInserterBolt(String mode) {
//		this.mode  = mode ;
//	}
	
	public ESInserterBolt() {}

	@Override
	public void execute(Tuple tuple) {
				
		if ( ! isTickTuple(tuple)) {
			
			if ( INSERT_INTO_ES_ENABLED ) {
				String index = tuple.getValue(0).toString();
				String json = tuple.getValue(1).toString();
				
				LOG.debug("Send to ELK: {}" ,tuple);
				bulkProcessor.add(
		        		new IndexRequest(index, "_doc")
		        			.source(json, XContentType.JSON)
		        		);
			}

	        countExecNanos++;
			counter++;
			counterV2++;
			
		} else {
			LOG.debug("## Tick has been received ##");
			 
			if ( numTicks == 0 ) {
				tsSpeedV2 = System.currentTimeMillis() ;
				counterV2 = 0 ;
				tsSpeed = System.currentTimeMillis() ;
//		        nanosExecInsertES = 0 ;
		        countExecNanos = 0 ;
			}

			numTicks++;


			if ( numTicks%120 == 0 ) {
				try {
					LOG.info(
							String.format("ESINSERTER V2: speed: %d recs/s." ,
									(1000*counterV2)/(System.currentTimeMillis() - tsSpeedV2)
							)
					);
				} catch (Exception ex){}
					
				tsSpeedV2 = System.currentTimeMillis() ;
				counterV2 = 0 ;
			}
		}
		
		_collector.ack(tuple);
	}


	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector outputcollector) {
		_collector = outputcollector ;
        propsLookup = conf ;
        
        ArrayList<String> keys = new ArrayList<String>() ;
        for (Object key: propsLookup.keySet()) {
        	keys.add(key.toString()) ;
        }
        Collections.sort(keys); 
        for ( String key: keys) {
        	LOG.info("ESInserterBolt property [{}] value [{}]", key, propsLookup.get(key));
        }
        
        loadMainParameters();

		if ( bulkProcessor  == null ) {
			bulkProcessor = MyESUtils.build(propsLookup) ;
		}
	}

	/**
	 * Method would be used if the bolt were to emit a new Tuple for processing by another bolt
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		Fields fObj = new Fields("index", "type", "data")	 ;	
		ofd.declare(fObj);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
		return conf;
	}		

	protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(org.apache.storm.Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(org.apache.storm.Constants.SYSTEM_TICK_STREAM_ID);
    }

	
    private void loadMainParameters() {
        String auxP = (String) propsLookup.get("disable_insert_es");
        if ( auxP != null && auxP.toString().compareToIgnoreCase("true") == 0  ){
        	INSERT_INTO_ES_ENABLED  = false ;
        	LOG.warn("## INSERT INTO ELASTICSEARCH DISABLED!!!!!!" );
        }
    }
}
