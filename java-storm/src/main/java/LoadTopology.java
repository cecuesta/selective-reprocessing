import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import java.io.FileReader;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import edu.doc_ti.jfcp.selec_reproc.storm.bolt.ESInserterBolt;
import edu.doc_ti.jfcp.selec_reproc.storm.bolt.ProcessBolt;
import edu.doc_ti.jfcp.selec_reproc.utils.Constants;

/**
 * This example sets up 3 topologies to put data in Kafka via the KafkaBolt,
 * and shows how to set up a topology that reads from some Kafka topics using the KafkaSpout.
 */
public class LoadTopology {

    private static String TOPIC_NAME = "topic_data" ;
	private static String KAFKA_BROKERS = "kafka:9092";
	private static String nimbusHosts = "" ;
	
	private static int    numWorkers = -1 ;
	private static int    kafkaBPerWorker = -1 ;	
	private static int    processPerWorker = -1 ;
	private static int    esInsertPerWorker = -1 ;	
	

    public static void main(String[] args) throws Exception {
        new LoadTopology().runMain(args);
    }

    private static String getPropNotNull(Properties p, String key) {
    	String value = null ;
    	value = p.getProperty(key) ;
    	
    	if ( value == null) {
    		System.err.println("Property " + key + " is not defined");
    		System.exit(-1) ;
    	} else {
    		System.out.println(String.format("Key[%s] = [%s]", key, value));
    	}
    	
    	return value ;
    }    
    static Properties gProps = new Properties() ;
	private String KAFKA_CONSUMER_GROUP;
	
    protected void runMain(String[] args) throws Exception {
 
		if ( args.length < 2) {
			System.err.println("Usage: propsfile taskname");
			System.exit(-1);
		}

		String propsName = args[0];
        String taskName = args[1];
		
		gProps.load(new FileReader(propsName)) ;
		
		if ( gProps.isEmpty()) {
			System.err.println(String.format("Could not read properties file: %s", propsName));
			System.exit(-1) ;
		}
		
		TOPIC_NAME = getPropNotNull( gProps, Constants.K_TOPIC_NAME) ;
		KAFKA_CONSUMER_GROUP = getPropNotNull( gProps, Constants.K_KAFKA_CONSUMER_GROUP ) ;
	
		KAFKA_BROKERS = getPropNotNull( gProps, Constants.K_KAFKA_BOOTSTRAPSERVERS);
        nimbusHosts = getPropNotNull( gProps, Constants.K_STORM_NIMBUS_HOST) ;
		
        numWorkers = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUMWORKERS) ) ;
        kafkaBPerWorker = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUM_KAFKA_SPOUTS) ) ;
        processPerWorker = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUM_PROCESS_BOLTS) ) ;
        esInsertPerWorker = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUM_ES_BOLTS) ) ; 

        
        Config tpConf = getConfig(args);

        //Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
        StormSubmitter.submitTopology(taskName, tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(KAFKA_BROKERS)));
    }

    protected Config getConfig(String[] args) {
        Config config = new Config();

        for ( Enumeration<Object> en = gProps.keys(); en.hasMoreElements(); ) {
        	Object key = en.nextElement() ;
        	config.put((String) key, gProps.get(key)) ;
        	System.out.println( "INPUT PARAMETER: " + String.format("[%s] : [%s]",(String) key, gProps.get(key)) );
        }
        config.put(Constants.K_STORM_CONFIG_FILE, args[0]) ;        
        
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+IgnoreUnrecognizedVMOptions");
        
        return config;
    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("process_bolt", new ProcessBolt()).shuffleGrouping("kafka_spout", TOPIC_NAME);
        
        
//        tp.setBolt("es_bolt", new ProcessBoltDummy()).shuffleGrouping("process_bolt");
        tp.setBolt("es_bolt", new ESInserterBolt()).shuffleGrouping("process_bolt");
        
                
        
        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
//        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
//                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
//                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_NAME);

    	ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values( r.value()),
                new Fields("value"), TOPIC_NAME);

        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{TOPIC_NAME})
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP)
            .setRetry(getRetryService())
            .setRecordTranslator(trans)
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
            TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }
}