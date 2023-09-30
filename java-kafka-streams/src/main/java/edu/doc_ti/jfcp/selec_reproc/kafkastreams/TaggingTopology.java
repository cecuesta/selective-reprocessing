/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.doc_ti.jfcp.selec_reproc.kafkastreams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text;
 * the code split each text line in string into words and then write back into a sink topic "streams-linesplit-output" where
 * each record represents a single word.
 */
public class TaggingTopology {
	public static int numRecords = 100 ;
	
	public static JdbcInserter mysqlIns ;

    public static void main(String[] args) {
    	
		String topicIn = "topic_in_stream" ; 
		String topicOut = "topic_data" ; 
		String bootstrapServers = "127.0.0.1:9092" ;
		String defURLmysql = "jdbc:mysql://localhost/mydatabase?user=myuser&password=rootpass" ;
		
		Options options = new Options();
		options.addOption(new Option("h", "help", false, "Print this help"));
		options.addOption(new Option("n", "numrecords", true, "Make a different tag every N records (def: 10000)"));
		options.addOption(new Option("b", "broker", true, "List of kafka bootstrap servers (def: 127.0.0.1:9092)"));
		options.addOption(new Option("i", "topic_in", true, "Input topic name (default: topic_in_stream)"));
		options.addOption(new Option("o", "topic_out", true, "Output topic name (default: topic_data)"));
		options.addOption(new Option("u", "mysqlurl", true, "URL for mysql (default: '" + defURLmysql + "')"));
    	
    	
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null ;
		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}
		
		if ( cmd.hasOption('n')  ) {
			try {
				numRecords = Integer.parseInt(cmd.getParsedOptionValue("n").toString());
			} catch (Exception e) {
			}
		} 
		if ( cmd.hasOption('i')  ) {
			try {
				topicIn = cmd.getParsedOptionValue("i").toString() ;
			} catch (Exception e) {
			}
		} 
		if ( cmd.hasOption('o')  ) {
			try {
				topicOut = cmd.getParsedOptionValue("o").toString() ;
			} catch (Exception e) {
			}
		}
		
		if ( cmd.hasOption('b')  ) {
			try {
				bootstrapServers = cmd.getParsedOptionValue("b").toString() ;
			} catch (Exception e) {
			}
		}
		
		if ( cmd.hasOption('u')  ) {
			try {
				defURLmysql = cmd.getParsedOptionValue("u").toString() ;
			} catch (Exception e) {
			}
		}
				
		mysqlIns = new JdbcInserter(defURLmysql) ;
  
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tagging-topology" );
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1") ;
        
        Topology builder = new Topology();

     builder.addSource("source", topicIn)
         .addProcessor("process", () -> new ProcessorTagging(), "source")
         .addSink("sink", topicOut, "process") 
         ;        

     final KafkaStreams streams = new KafkaStreams(builder, props);
     
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
