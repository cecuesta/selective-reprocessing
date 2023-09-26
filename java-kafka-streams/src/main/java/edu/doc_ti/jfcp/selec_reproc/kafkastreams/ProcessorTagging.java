package edu.doc_ti.jfcp.selec_reproc.kafkastreams;

import java.util.UUID;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ProcessorTagging implements Processor<String, String, String, String> {

    ProcessorContext<String, String> _context ;
    
    String baseID = UUID.randomUUID().toString() ;
    int counterTags = 1 ;
    int counterRecords = 0 ;
    String currentTag = baseID + "-" + Integer.toString(counterTags) ;
   
	@Override
    public void init(final ProcessorContext<String, String> context) {
    	
    	_context = context ;
    	
//        context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
//            try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
//                while (iter.hasNext()) {
//                    final KeyValue<String, Integer> entry = iter.next();
//                    context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
//                }
//            }
//        });
//        kvStore = context.getStateStore("Counts");
    }

    @Override
    public void process(final Record<String, String> record) {
    	
    	counterRecords++ ;
    	
    	
//    	System.out.println("INPUT: "  + record.value() ) ;
//    	System.out.println( "THREAD: " + Thread.currentThread().getName() ) ;
    	
    	Record<String, String> recordOut = new Record<String, String>(record.key(),
    			currentTag + ";" + Integer.toString( counterRecords) + ";" +
    			record.value(), record.timestamp()) ;
		_context.forward(recordOut);

		if ( counterRecords % TaggingTopology.numRecords == 0 ) {
    		closeTag() ;
    	}
    }

    private void closeTag() {

    	System.out.println( "closeing TAG " + currentTag + " with " + counterRecords );
    	
    	TaggingTopology.mysqlIns.insert(currentTag, counterRecords);
    	
    	// FALTA MANDAR a MYSQL
    	counterRecords = 0 ;
    	counterTags++ ;
    	currentTag = baseID + "-" + Integer.toString(counterTags) ;
	}

	@Override
    public void close() {
    	
		closeTag(); 
    	System.out.println("CLOSING PROCESSOR " + this.toString());
    }


}
