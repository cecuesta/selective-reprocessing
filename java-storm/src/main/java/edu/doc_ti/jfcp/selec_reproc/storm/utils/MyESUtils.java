package edu.doc_ti.jfcp.selec_reproc.storm.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class MyESUtils {

	//private static final Logger LOG = MiLogger.getLogger(MyESUtils.class, "1.0.1 - 2020/10/14A");
	private static final Logger LOG =  LogManager.getFormatterLogger(MyESUtils.class);
	public static RestHighLevelClient restHighLevelClient = null ;
	final static CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	
	//private static final Logger LOG = ;
	@SuppressWarnings("rawtypes")
	private static int checkInteger (Map props, String key, int defaultVal) {
		int result = defaultVal ;
		try {
			String aux = (String) props.get(key) ;
			if ( aux != null ) {
				result = Integer.parseInt(aux) ;
			}
		} catch (Exception ex) {
			LOG.warn("Integer expected in {} [{}]", key, props.get(key));
		}

		LOG.info("BulkProcessor property [{}] : [{}]", key, result ) ;
		return result ;
	}
	

@SuppressWarnings({ "rawtypes", "unused", "unchecked" })
private static RestHighLevelClient getRestHighLevelClient(Map props ) {
//	RestHighLevelClient restHighLevelClient = null ;
			/*SniffOnFailureListener sniffOnFailureListener =
		      new SniffOnFailureListener();*/
		if (restHighLevelClient==null) {
			try {			
				String list = (String) props.get(Constants.K_ELK_HOST) ;
				
				if ( list == null ) {
					LOG.fatal("Missing property: "+ Constants.K_ELK_HOST);
					System.exit(-1) ;
				}
				
				ArrayList<HttpHost> hostsTmp = new ArrayList<>();

				for ( String aux: list.split(",")) {
					LOG.info("------------: " + aux);
					
					if (aux.contains(":")) {
						String[] hostsPorts = aux.split(":");
						hostsTmp.add( new HttpHost(hostsPorts[0], Integer.parseInt(hostsPorts[1]), "http"));
						LOG.info("[{}] ELASTICSEARCH: Connecting to ES Node: [{}:{}]",  hostsPorts[0], hostsPorts[1]);
					} else {
						hostsTmp.add( new HttpHost(aux.trim(), 9200, "http"));
						LOG.info("[{}] ELASTICSEARCH: Connecting to ES Node: [{}:9200]",  aux.trim());					
					}
				}
				Collections.shuffle(hostsTmp);

				HttpHost[] hosts = new HttpHost[hostsTmp.size()] ;
				for (int t= 0 ; t<hostsTmp.size(); t++) { 
					hosts[t] = hostsTmp.get(t) ;
				}
				//ELK Auth
				
				String elk_user = (String) props.getOrDefault(Constants.K_ELK_USER, "") ;
				String elk_password = (String) props.getOrDefault(Constants.K_ELK_PASSWD, "") ;
				credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elk_user, elk_password));
				RestClientBuilder builder = RestClient
						.builder(hosts)
						//.setFailureListener(sniffOnFailureListener)
					    .setRequestConfigCallback(
					        new RestClientBuilder.RequestConfigCallback() {
					            @Override
					            public RequestConfig.Builder customizeRequestConfig(
					                    RequestConfig.Builder requestConfigBuilder) {
					                return requestConfigBuilder
					                    .setConnectTimeout(5000)
					                    .setSocketTimeout(60000);
					            }
					        })
						.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
			            @Override
				            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				                return httpClientBuilder
				                		.setDefaultCredentialsProvider(credentialsProvider)
				                		.setDefaultIOReactorConfig(
				                				IOReactorConfig.custom()
						                        .setIoThreadCount(5)
						                        .build())
						        ;
				            }
//			            @Override
//			                    IOReactorConfig.custom()
//			                        .setIoThreadCount(1)
//			                        .build());
			                
			            
				        });
				
				restHighLevelClient = new RestHighLevelClient(builder);	
				/*sniffOnFailureListener.setSniffer(sniffer);*/
				
				NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(restHighLevelClient.getLowLevelClient(),
						ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
						ElasticsearchNodesSniffer.Scheme.HTTP);
				
				Sniffer sniffer = Sniffer.builder(restHighLevelClient.getLowLevelClient())
						 .setNodesSniffer(nodesSniffer)
						 .setSniffIntervalMillis(10000)
						 .setSniffAfterFailureDelayMillis(10000)
						 .build();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1) ;
			}
		}	
		
	     
		return restHighLevelClient;
	}

    public static BulkProcessor build(Map<?, ?> props) {
		/* Needed Properties in Map props
		 * Constants.K_ELK_CLUSTER_NAME
		 * Constants.K_ELK_HOST
		 * Constants.K_ELK_BATCH_SIZE
		 * Constants.K_ELK_BATCH_TAM_MB
		 * Constants.K_ELK_BATCH_FLUSH_SECS
		 * Constants.K_ELK_BATCH_CONCURRENT
		 * 
		 */

		int numBulkActions 			= checkInteger(props, Constants.K_ELK_BATCH_SIZE,    	1000) ;
		int batchSizeMB    			= checkInteger(props, Constants.K_ELK_BATCH_TAM_MB,  	1 ) ;
		int flushIntervalSeconds 	= checkInteger(props, Constants.K_ELK_BATCH_FLUSH_SECS, 5 ) ;
		int numConcurrentRequests 	= checkInteger(props, Constants.K_ELK_BATCH_CONCURRENT, 0 ) ;

		LOG.info("We are using version 7.9.3");
        System.out.println("We are using version 7.9.3");
		LOG.info("ELASTICSEARCH: Batch records: {}", numBulkActions) ;
		LOG.info("ELASTICSEARCH: Batch size MB: {}", batchSizeMB) ;
		LOG.info("ELASTICSEARCH: Batch flush interval seconds: {}", flushIntervalSeconds) ;
		LOG.info("ELASTICSEARCH: Batch Concurrent requests: {}", numConcurrentRequests) ;
		

		String list = (String) props.get(Constants.K_ELK_HOST) ;
		
		
		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
	      	long numRegsAcum = 0 ;
	      	long tiempoTotal = 0 ;

	          @Override
	          public void beforeBulk(long executionId, BulkRequest request) { 
	          } 

	          @Override
	          public void afterBulk(long executionId, BulkRequest request, BulkResponse response) { 
	        	  if ( response.hasFailures() ) {
	        		  LOG.error( "BULK indexing Failure: {}", response.buildFailureMessage());
	        		  for (int i = 0; i < response.getItems().length; i++) {
	                      BulkItemResponse item = response.getItems()[i];
	                      if (item.isFailed()) {
	                    	  IndexRequest ireq = (IndexRequest) request.requests().get(i);
	                          LOG.error("Failed while indexing to {} type {} request: [{}]: [{}]", 
	                        		  	item.getIndex(), item.getType(), ireq, item.getFailureMessage() );
	                      }
	                  }
	          	  }
	              numRegsAcum += response.getItems().length ;
	              tiempoTotal += response.getIngestTookInMillis() ;
	              LOG.debug("Regs/seg: {}, Regs: {}, {}" ,
	                  		( (1000*numRegsAcum)/tiempoTotal ),
	                  		response.getItems().length,
	                  		Thread.currentThread().getName()
	                  		) ;
	          } 
	          @SuppressWarnings("unused")
			@Override
	          public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
	              System.out.println("Bulk execution failed ["+  executionId + "].\n" + failure.toString() ) ;
	              for (int i = 0; i < request.requests().size(); i++) {
                      DocWriteRequest<?> item = request.requests().get(i);
                      //System.out.println("Bulk execution failed, request = " + item.toString());
                  }
	              failure.printStackTrace();
	          } 
	      };
	      LOG.debug("*******************************getRestHighLevelClient************************************");
	      
		      //Async
	    	  return BulkProcessor.builder((request, bulkListener) ->
	      		{
	      			RestHighLevelClient  restHighLevelClient=getRestHighLevelClient(props);
	      			//sniffOnFailureListener.setSniffer(sniffer);
					
	      			restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
				}, listener)
		        .setBulkActions(numBulkActions) 
		        .setBulkSize(new ByteSizeValue(batchSizeMB, ByteSizeUnit.MB)) 
		        .setFlushInterval(TimeValue.timeValueSeconds(flushIntervalSeconds)) 
		        .setConcurrentRequests(numConcurrentRequests)
		        .build(); 
	}
	
	
    public static void closeBP( BulkProcessor bp ) {
    	
    	boolean terminated = false ;
    	bp.flush();
    	while ( !terminated ) {
    		try {
				terminated = bp.awaitClose(5L, TimeUnit.SECONDS);
			} catch (InterruptedException e) {}
    	}
    	
    	bp.close() ;
    }
    
    
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		
		Map<String, String> props = new HashMap<String, String>();
		
//		props.put("bulk.hosts", "10.192.23.140:19199" );
		props.put(Constants.K_ELK_HOST, "elasticsearch:9200" );
//		props.put("bulk.clustername", "mirador") ;
		props.put("elk.bulk.sync", "false") ;
		
		System.out.println("------------------------") ;
		System.out.println(props) ;
		System.out.println("------------------------") ;
		
		BulkProcessor bulkp = MyESUtils.build(props) ;
		
		HashMap<String,Object> m = new HashMap<String,Object>() ;
		m.put("table", "tabla" ) ;
		m.put("campo1", "valor1" ) ;
		m.put ("int1", new Integer(1)) ;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z") ;
		m.put("fecha1", new Date() );
		m.put("fecha2", new Date(System.currentTimeMillis()) );
		m.put("fecha3", sdf.format( new Date(System.currentTimeMillis()) ) );
		
		for (int regs = 1 ; regs <= 1000 ; regs++) {
        	if ( regs%100 == 0 ) {
        		System.out.println ("Regs = " + regs ) ;  
        		bulkp.flush();
        		try {
        			Thread.sleep(60000);
        		} catch (InterruptedException e) {
        			e.printStackTrace();
        		}
        	}
        	bulkp.add(
    	    		new IndexRequest("test-index", "_doc").source(m)  
        		) ;
        }
		bulkp.close();

        System.out.println("Inserted all records in test-index") ;
        
	}
}
