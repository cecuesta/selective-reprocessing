package edu.doc_ti.jfcp.selec_reproc.gendata;

import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaGenerator {
    private static final Logger log = LoggerFactory.getLogger(KafkaGenerator.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        int speed = 500 ;
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "topic_in_stream" ;
        
		Options options = new Options();
		
		options.addOption(new Option("h", "help", false, "Print this help"));
		options.addOption(new Option("d", "date", true, "Date for the input data, format [yyyy-mm-dd]"));
		options.addOption(new Option("n", "numrecords", true, "Generate n records per second (default " + speed + ")"));
		options.addOption(new Option("b", "broker", true, "List of kafka bootstrap servers (default: " + bootstrapServers + ")" ));
		options.addOption(new Option("t", "topic", true, "Topic name (default: "+ topic +")"));
        
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null ;
		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}

		if ( cmd.hasOption('h') || cmd.getOptions().length < 0 ) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("KafkaGenerator", options);
			System.exit(0) ;
		}
       
		try {
			FileGenerator.tsFrom = FileGenerator.sdf.parse(FileGenerator.sdf.format(new Date() ));
		} catch (ParseException e) {}
		if ( cmd.hasOption('d')  ) {
			try {
				FileGenerator.tsFrom = FileGenerator.sdf.parse(cmd.getParsedOptionValue("date").toString());
			} catch (Exception e) {
			}
		} 
		
		FileGenerator.tsTo = new Date( FileGenerator.tsFrom.getTime() + 24*3600*1000 - 1000);
        
		
		if ( cmd.hasOption('n')  ) {
			try {
				speed = Integer.parseInt(cmd.getParsedOptionValue("n").toString());
			} catch (Exception e) {
			}
		} 
		
		if (speed > 5000) {
			speed = 5000 ;
		}
		
		if ( cmd.hasOption('t')  ) {
			try {
				topic = cmd.getParsedOptionValue("t").toString() ;
			} catch (Exception e) {
			}
		} 
		if ( cmd.hasOption('b')  ) {
			try {
				bootstrapServers = cmd.getParsedOptionValue("b").toString() ;
			} catch (Exception e) {
			}
		} 
		
//		-Dorg.slf4j.simpleLogger.defaultLogLevel=debug
				
				
         // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(speed)  );

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        
        while ( true ) {
        	long t0 = System.currentTimeMillis();
        	for ( int nn = 1 ; nn<= speed; nn++ ) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, FileGenerator.getData(1).replaceAll("\"", "").replace("\n", ""));
                producer.send(producerRecord);
        	}
            // flush data - synchronous
            producer.flush();
            
            log.info("Flush to " + topic + " "+ speed + " records") ;
            
            while (System.currentTimeMillis() - t0 < 1000) {
            	try {
					Thread.sleep(1) ;
				} catch (InterruptedException e) {}
            }

        }

    }
}