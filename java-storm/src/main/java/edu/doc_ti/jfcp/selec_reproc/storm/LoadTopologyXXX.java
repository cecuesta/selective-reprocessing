
package edu.doc_ti.jfcp.selec_reproc.storm ;

import java.io.FileReader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.format.SimpleFileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.doc_ti.jfcp.selec_reproc.storm.bolt.BoltBuilder;
import edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants;
import edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys;

public class LoadTopologyXXX {
    public static final Logger LOG = LoggerFactory.getLogger(LoadTopologyXXX.class);

    private BrokerHosts brokerHosts = null ;

    private static String topicName = "testtopic_4" ;
	private static String zookeeperRoot4Kafka = "storm/checkpoint" ;
	private static String kafkaZookeeper = "" ;
	private static String bootstrapKafkaServers = "";
	private static String nimbusHosts = "" ;
	
	private static int    numWorkers = -1 ;
	private static int    kafkaBPerWorker = -1 ;	
	private static int    processPerWorker = -1 ;
	private static int    esInsertPerWorker = -1 ;
	
    public LoadTopologyXXX(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
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
    
	public StormTopology buildTopology(Config config) {
    	
    	System.out.println("##################################################################################") ;
    	System.out.println("# V 1.0.0A ");
    	System.out.println("# Num Workers: " + numWorkers) ;
    	System.out.println("# kafka readers: " + (numWorkers*kafkaBPerWorker)) ;
    	System.out.println("# process bolts: " + (numWorkers*processPerWorker)) ;
    	System.out.println("# insert ES bolts: " + (numWorkers*esInsertPerWorker)) ;
    	System.out.println("# kafka topic to read: " + topicName) ;
    	System.out.println("##################################################################################") ;

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topicName, "", zookeeperRoot4Kafka);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        kafkaConfig.bufferSizeBytes = 10*1024*1024 ;
//        kafkaConfig.fetchSizeBytes  = 10*1024*1024 ;
//        kafkaConfig.stateUpdateIntervalMs = 20000 ;
        
		
//		Map confESBolt = new HashMap();
//    	confESBolt.put("es.input.json", "true"); 
//    	confESBolt.put("es.nodes", ELKHost ) ;
//    	confESBolt.put("es.batch.size.entries", ELKBatchSize );
//    	confESBolt.put("es.batch.size.bytes", "10m") ;
		

        TopologyBuilder builder = new TopologyBuilder();
        BoltBuilder boltBuilder = new  BoltBuilder();
    	
    	builder.setSpout(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.KAFKA_SPOUT_ID, 
    			new KafkaSpout(kafkaConfig), 
    			numWorkers*kafkaBPerWorker);    
        builder.setBolt(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.PROCESS_BOLT_ID, 
        		boltBuilder.buildProcessBolt(), 
        		numWorkers*processPerWorker)
					.shuffleGrouping(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.KAFKA_SPOUT_ID);
    	builder.setBolt(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INSERT_BOLT_ID, 
    			boltBuilder.buildESInserterBolt(), 
    			numWorkers*esInsertPerWorker)
    				.shuffleGrouping(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.PROCESS_BOLT_ID,"stream-" + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INSERT_BOLT_ID);	
    	

    	
        return builder.createTopology();
    }

    static Properties gProps ;


	public static void main(String[] args) throws Exception {

		if ( args.length < 3) {
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
		
	    topicName = getPropNotNull( gProps, Constants.K_TOPIC_NAME) ;
		zookeeperRoot4Kafka = getPropNotNull( gProps, Constants.K_STORM_ZOOKEEPER_ROOT ) ;
//		ELKHost = getPropNotNull( gProps, Constants.K_ELK_HOST ) ;
//		ELKBatchSize = getPropNotNull( gProps, Constants.K_ELK_BATCH_SIZE ) ;
		kafkaZookeeper = getPropNotNull( gProps, Constants.K_KAFKA_ZOOKEEPER ) ;
        bootstrapKafkaServers = getPropNotNull( gProps, Constants.K_KAFKA_BOOTSTRAPSERVERS);
        nimbusHosts = getPropNotNull( gProps, Constants.K_STORM_NIMBUS_HOST) ;
//        loadMode = getPropNotNull( gProps, Constants.K_LOAD_MODE) ;      
		
        numWorkers = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUMWORKERS) ) ;
        kafkaBPerWorker = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUM_KAFKA_SPOUTS) ) ;
        processPerWorker = Integer.parseInt( getPropNotNull( gProps, Keys.K_NUM_PROCESS) ) ;
        esInsertPerWorker = Integer.parseInt( getPropNotNull( gProps, Constants.K_NUM_ES_BOLTS) ) ; 


        LoadTopologyXXX myTopology = new LoadTopologyXXX(kafkaZookeeper);
        Config config = new Config();
       
        String str_maxHeapSize = gProps.getProperty(Keys.WORKER_MAXHEAPSIZE) ;
        if (!Strings.isNullOrEmpty(str_maxHeapSize)){
        	try {
        		int maxHeapSize = Integer.parseInt(str_maxHeapSize) * 1024;
                config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, maxHeapSize); 
        	} catch (NumberFormatException nfe){}   
        }
          
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, getPropNotNull( gProps, Keys.WORKER_CHILDOPTS));
//        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000000) ;
//        config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60) ;
//        config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 3) ;
        
//        config.setMaxSpoutPending(1000000);
//        config.setMessageTimeoutSecs(1);
//        config.setNumAckers(0);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);
//        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000000);
        
//        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1048576);
//        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 1048576);
//        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = myTopology.buildTopology(config);

        if (args != null && args.length > 1) {

            config.setNumWorkers(numWorkers);
            config.setMaxTaskParallelism(400);
//            config.setMaxSpoutPending(6000000);
            config.put(Config.NIMBUS_SEEDS, Arrays.asList(nimbusHosts));
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);

            for ( Enumeration<Object> en = gProps.keys(); en.hasMoreElements(); ) {
            	Object key = en.nextElement() ;
            	config.put((String) key, gProps.get(key)) ;
            	System.out.println( "INPUT PARAMETER: " + String.format("[%s] : [%s]",(String) key, gProps.get(key)) );
            }
            config.put(Constants.K_STORM_CONFIG_FILE, args[0]) ;

            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(kafkaZookeeper));
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

//            config.put(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, 200000000 );
            StormSubmitter.submitTopology(taskName, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(kafkaZookeeper));
            cluster.submitTopology("kafka", config, stormTopology);
        }
        
    }
	
	

}