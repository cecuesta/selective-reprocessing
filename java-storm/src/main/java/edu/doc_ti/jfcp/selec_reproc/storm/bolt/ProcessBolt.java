package edu.doc_ti.jfcp.selec_reproc.storm.bolt;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.base.Strings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.doc_ti.jfcp.selec_reproc.storm.model.CDRMetadata;
import edu.doc_ti.jfcp.selec_reproc.storm.model.CDRRecord;
import edu.doc_ti.jfcp.selec_reproc.storm.utils.CasesManager;
import edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants;


public class ProcessBolt extends BaseRichBolt {
		
	/**
	 * 
	 */
	private static final long serialVersionUID = 5159756939464857116L;
	
	private static Logger LOG = LoggerFactory.getLogger(ProcessBolt.class);
	private CDRMetadata metadata;
	private CasesManager casesManager;
	private OutputCollector _collector;
	private String indexBaseName;
	private String docTypeName;
	private String fromKafkaTopicName;
	private String toKafkaBaseTopicName;
	private String qciDefaultValue;
	private String anchorDefaultValue;
	private String forceNumber2Process;
	private int counter;

    private boolean globalInsertInKafkfa = true ;
    private boolean globalInsertInES = true ;
    private long t0;
	
	long lastTimeReceivedTuple = 0 ;
	private int timeoutWriteFiles;
	private boolean writeFiles;
	
	public ProcessBolt(){}

	/**
	 * Check if the tuple is a tick tuple
	 * @param tuple
	 * @return
	 */
	protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
	
	@Override
	public void cleanup() {

	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// configure how often a tick tuple will be sent to our bolt
		Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        LOG.debug("## Configure Storm with Tick Tuple Configuration ##");
        
        return conf;
    }


	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		String[] fields = (String[]) edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.MAP_FIELDS_REDIRECT.values().toArray(new String[0]);
		Fields fObj = new Fields(Arrays.asList(fields));
//		ofd.declare(fObj);
		
		ofd.declareStream("stream-" + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INSERT_BOLT_ID, fObj);
		ofd.declareStream("stream-" + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.KAFKA_INSERT_BOLT_ID, fObj);

		String[] fields2 = {"json"} ;
		Fields fObj2 = new Fields(Arrays.asList(fields2));
		
		
		ofd.declareStream("stream-json-hdfs", fObj2);
	}

	
	long tAnt = 0 ;

	private String boltID;

	@Override
	public void execute(Tuple tuple) {
	    if (isTickTuple(tuple)) {
	    	LOG.debug("## Tick has been received ##");
	    	    
	    	_collector.ack(tuple);
	       return;
	    }
	    
	    lastTimeReceivedTuple = System.currentTimeMillis() ;
	    
	    LOG.debug("Executing tuple: {}", tuple);

	    CDRRecord cdr = new CDRRecord(
	    		getIndexBaseName(), 
	    		getDocTypeName(), 
	    		getToKafkaBaseTopicName(),
	    		getQciDefaultValue(),
	    		getAnchorDefaultValue(),
	    		getForceNumber2Process(),
	    		metadata,
	    		casesManager);

	    try {
	    	cdr.setObjectToRedirect(tuple.getValue(0));
		    if (cdr.getCdrData().get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.NOPROCESS_VALUE) == null){
		    	
		    	if (cdr.getCdrData().get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR) == null){
			    	cdr.enrichObjectToRedirect();
			    }
		    	cdr.getCdrData().put(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.BOLTID, boltID) ;
		    	
			    cdr.setIndexToRedirect();
			    cdr.setTopicToRedirect();

			    String index = cdr.getIndex();
			    String docType = cdr.getDocTypeName();
			    String file = cdr.getFile();
			    
			    boolean insertInKafka = true;
			    boolean insertInES = true;
                	if (file.startsWith(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.RELOAD_FILE_PRFX)){
		    		file = file.replaceAll(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.RELOAD_FILE_PRFX, "");
		    		cdr.setFile(file);
		    		cdr.getCdrData().put(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.FILENAME, file) ;
	    			insertInKafka = false;			    		 
		    	}
		    	
		    	String outKafkaTopic = cdr.getToKafkaTopic();
			    String objToELK = cdr.getJsonStrToELK();
		    	
			    if (outKafkaTopic.startsWith(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INDEX_TOPIC_ERR)){
			    	
			    	cdr.getCdrData().put(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.RAWDATA, tuple.getValue(0).toString()) ;
			    	
			    	 if (globalInsertInES ){
			    		 _collector.emit("stream-" + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INSERT_BOLT_ID, 
				    			 new Values(index, 
					 	    		docType, 
					 	    		file, 
					 	    		outKafkaTopic, 
					 	    		objToELK ) ) ;
			    	 }
			    	 
			    }
			    
			    if ( counter%1000 == 0  ) {
			    	if (counter == 0 ) {
			    		t0 = System.currentTimeMillis() ;
			        } else {
			        	System.out.println("CDR procesado con fecha: " + (new Date(cdr.getTimestamp())) + " ----> Tiempo de proceso : " + counter + " " + (System.currentTimeMillis() - t0 ) + " " + (System.currentTimeMillis() - tAnt ));
			        	tAnt = System.currentTimeMillis();
			        }
			    }

			    counter++;
		    }
	    } catch (Exception e) {
	    	LOG.error("Process error: ", e);
	    }
	    
	    	    
//	    LOG.debug("To next phase... {}=[{}], {}=[{}], {}=[{}], {}=[{}], {}=[{}], ", 
//	    		com.hpe.cms.mirador.storm.utils.Constants.MAP_FIELDS_REDIRECT.get(com.hpe.cms.mirador.storm.utils.Constants.INDEX_POSITION), index, 
//	    		com.hpe.cms.mirador.storm.utils.Constants.MAP_FIELDS_REDIRECT.get(com.hpe.cms.mirador.storm.utils.Constants.DOCTYPE_POSITION), docType, 
//	    		com.hpe.cms.mirador.storm.utils.Constants.MAP_FIELDS_REDIRECT.get(com.hpe.cms.mirador.storm.utils.Constants.FILENAME_POSITION), file, 
//	    		com.hpe.cms.mirador.storm.utils.Constants.MAP_FIELDS_REDIRECT.get(com.hpe.cms.mirador.storm.utils.Constants.OUT_TOPIC_POSITION), outKafkaTopic, 
//	    		com.hpe.cms.mirador.storm.utils.Constants.MAP_FIELDS_REDIRECT.get(com.hpe.cms.mirador.storm.utils.Constants.DATA_POSITION), objToRedirect);   
//	    
	    
	    // Emit the tuple to ESInserterBolt, KafkaBolt and/or HDFSBolt
//	    _collector.emit(new Values(index, 
//	    		docType, 
//	    		file, 
//	    		outKafkaTopic, 
//	    		objToRedirect ) ) ;
	    
	    _collector.ack(tuple);	
	}
	
	/**
	 * Called just before the bolt starts processing tuples
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void prepare(Map stormConf, TopologyContext context, OutputCollector arg2) {
		LOG.info("## Starting Process Bolt ##");
		_collector = arg2 ;
		Map cdrProperties = stormConf;
		String msgx ;

//        boltID = context.getStormId() + "-" + context.getThisComponentId() + "-" + context.getThisTaskId() ;
		boltID = Integer.toString(context.getThisTaskId());
		
		//### TOPICO DE ENTRADA
		String topicInput = (String)cdrProperties.get(Constants.K_TOPIC_NAME);	   
		if (!Strings.isNullOrEmpty(topicInput)){
			setFromKafkaTopicName(topicInput);
		} else {
			String msg = "Property " + Constants.K_TOPIC_NAME + " not defined";
			System.err.println(msg) ;
			LOG.error(msg);
			System.exit(-1) ;
		}
		
		//### TOPICO DE SALIDA
		String topicOutput = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_BASE_TOPIC_NAME);
		if (!Strings.isNullOrEmpty(topicOutput)){
			setToKafkaBaseTopicName(topicOutput);
		} else {
			String msg = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_BASE_TOPIC_NAME + " not defined";
			System.err.println(msg) ;
			LOG.error(msg);
			System.exit(-1) ;
		}	

		//### TOPICO DE SALIDA HABILITADO
		try {
			globalInsertInKafkfa = Boolean.parseBoolean((String) cdrProperties.getOrDefault(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_BASE_TOPIC_ENABLED, "true")) ;
		} catch (Exception ex ) {}

		msgx = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_BASE_TOPIC_ENABLED + " = " + globalInsertInKafkfa ;
		LOG.info(msgx);
		

		
		
		//### TIPOS CDR
		String listCdrTypesStr = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_TYPES);
		if (Strings.isNullOrEmpty(listCdrTypesStr)){
			String msg = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_TYPES + " not defined";
			System.err.println(msg) ;
			LOG.error(msg);
			System.exit(-1) ;
		}
		
		//### NÚMERO CAMPOS CDR FORZADOS A PROCESARSE
		String forceNumber2Process = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FORCE_NUMBER_PROCESS);
		if (!Strings.isNullOrEmpty(forceNumber2Process)){
			setForceNumber2Process(forceNumber2Process);
		} else {
			setForceNumber2Process("0");
			String msg = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FORCE_NUMBER_PROCESS + " not defined";
			LOG.info(msg);
		}


        //### TOPICO DE SALIDA HABILITADO
        try {
            globalInsertInKafkfa = Boolean.parseBoolean((String) cdrProperties.getOrDefault(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_BASE_TOPIC_ENABLED, "true")) ;
        } catch (Exception ex ) {}

        msgx = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_BASE_TOPIC_ENABLED + " = " + globalInsertInKafkfa ;
        LOG.info(msgx);
        

        //### INDICE A ELK

        try {
            globalInsertInES = Boolean.parseBoolean((String) cdrProperties.getOrDefault(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_ELK_INSERT_ENABLED, "true")) ;
        } catch (Exception ex ) {}

        msgx = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_ELK_INSERT_ENABLED + " = " + globalInsertInES ;
        LOG.info(msgx);

        
        
        
        
		
		//### CAMPOS CDR DATO
		String numberCdrDatoFieldsStr = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_DATO_NUMBER);		
		int numberCdrDatoFields = 0;
		if (Strings.isNullOrEmpty(numberCdrDatoFieldsStr)){
			LOG.warn(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_DATO_NUMBER + " not defined !!!!!!");
		} else {
			try{
				numberCdrDatoFields = Integer.valueOf(numberCdrDatoFieldsStr);
			} catch (NumberFormatException nfe){
				String msg = edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_DATO_NUMBER + " not defined properly !!!!!!";
				System.err.println(msg) ;
				LOG.error(msg);
				System.exit(-1) ;
			}
		}	
		
		String cdrsDatoNotPrintStr = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_DATO_NOTPRINT);
		
		
		//### CAMPOS CDR VOZ
		String numberCdrVozFieldsStr = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_VOZ_NUMBER);		
		int numberCdrVozFields = 0;
		if (Strings.isNullOrEmpty(numberCdrVozFieldsStr)){
			LOG.warn(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_VOZ_NUMBER + " not defined !!!!!!");
		} else {
			try{
				numberCdrVozFields = Integer.valueOf(numberCdrVozFieldsStr);
			} catch (NumberFormatException nfe){
				String msg = edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_VOZ_NUMBER + " not defined properly !!!!!!";
				System.err.println(msg) ;
				LOG.error(msg);
				System.exit(-1) ;
			}
		}
		
		String cdrsVozNotPrintStr = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.CDRS_FIELDS_VOZ_NOTPRINT);


		//### INDICE A ELK

		try {
			globalInsertInES = Boolean.parseBoolean((String) cdrProperties.getOrDefault(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_ELK_INSERT_ENABLED, "true")) ;
		} catch (Exception ex ) {}

		msgx = "Property " + edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_OUT_ELK_INSERT_ENABLED + " = " + globalInsertInES ;
		LOG.info(msgx);

		
		String cdrsIndex = (String)cdrProperties.get(Constants.K_ELK_INDEX_NAME);
		if (Strings.isNullOrEmpty(cdrsIndex)){
			cdrsIndex = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INDEX_DEFAULT;
			LOG.warn("{} is not defined !!!!!!. Creating default index {}", Constants.K_ELK_INDEX_NAME, cdrsIndex);
		}
		if ( (cdrsIndex.split("/")).length != 2) {
			cdrsIndex = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INDEX_DEFAULT;
			LOG.warn("Index name incorrect: {}. Initializing to default index !!!!!!", cdrsIndex);			
		}
		
		String aux[] = cdrsIndex.split("/");
		String tmpIndexFormat = aux[0].replaceAll("\\*", "");
		tmpIndexFormat = tmpIndexFormat.endsWith("-") ? tmpIndexFormat : tmpIndexFormat.concat("-");
		setIndexBaseName(tmpIndexFormat);
		setDocTypeName(aux[1]);
		
		
		//### METADATA COMUN A TODOS LOS CDRS
		metadata = new CDRMetadata();	
		metadata.constructTypesManager(listCdrTypesStr, cdrProperties);
		
		
		//### FECHAS DE CDRS A PROCESAR
		long millisecondsKafkaInit = 0;
		String kInitDate = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_INIT_DATE);
		if (!Strings.isNullOrEmpty(kInitDate)){
			try {
				Date date = metadata.getSdfKafkaDates().parse(kInitDate);
				millisecondsKafkaInit = date.getTime();
				metadata.setProcessFromKafkaDate(millisecondsKafkaInit);
			} catch (ParseException e) { }
		}
		
		long millisecondsKafkaEnd = 0;
		String kEndDate = (String)cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.K_END_DATE);
		if (!Strings.isNullOrEmpty(kEndDate)){
			try {
				Date date = metadata.getSdfKafkaDates().parse(kEndDate);
				millisecondsKafkaEnd = date.getTime();
				metadata.setProcessToKafkaDate(millisecondsKafkaEnd);
			} catch (ParseException e) { }
		}
		
		if (numberCdrDatoFields > 0){
			metadata.constructFieldsManager(numberCdrDatoFields, 
					cdrProperties, 
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_DATO);
			if (!Strings.isNullOrEmpty(cdrsDatoNotPrintStr)){
				metadata.constructListNotPrint(
						edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_DATO,
						cdrsDatoNotPrintStr);
			}
		}
		if (numberCdrVozFields > 0){
			metadata.constructFieldsManager(numberCdrVozFields, 
					cdrProperties, 
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_VOZ);
			if (!Strings.isNullOrEmpty(cdrsVozNotPrintStr)){
				metadata.constructListNotPrint(
						edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_VOZ,
						cdrsVozNotPrintStr);
			}
		}
		
		//Manage Files
		timeoutWriteFiles = 60 ;
		try {
			timeoutWriteFiles = Integer.parseInt((String) cdrProperties.get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.WRITE_FILES_TIMEOUT)) ;
			if (timeoutWriteFiles < 10 ) timeoutWriteFiles = 10 ;
			if (timeoutWriteFiles > 300 ) timeoutWriteFiles = 300 ;
		} catch (Exception ex) {}
		
		writeFiles = Boolean.parseBoolean((String) cdrProperties.getOrDefault(edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.WRITE_FILES, "false")) ;	
		LOG.info("Parameter {} = {}", edu.doc_ti.jfcp.selec_reproc.storm.utils.Keys.WRITE_FILES, writeFiles) ;

		LOG.info ("Index used: " + getIndexBaseName() + "*/" + getDocTypeName()) ;
		LOG.info("Executing ProcessBolt...");
		
//		for (Object name: stormConf.keySet()){
//			if (name != null){
//	            String key = name.toString();
//	            String value = stormConf.get(name) != null ? stormConf.get(name).toString() : null;  
//	            LOG.info("## Storm configuration: key='{}', value='{}'", key, value);
//			}
//		}		
	}
	
	public void setIndexBaseName(String indexBaseName) {
		this.indexBaseName = indexBaseName;
    }
	
    public String getIndexBaseName() {
    	return indexBaseName ;
    }

	public String getDocTypeName() {
		return docTypeName;
	}

	public void setDocTypeName(String docTypeName) {
		this.docTypeName = docTypeName;
	}

	public String getFromKafkaTopicName() {
		return fromKafkaTopicName;
	}

	public void setFromKafkaTopicName(String fromKafkaTopicName) {
		this.fromKafkaTopicName = fromKafkaTopicName;
	}

	public String getToKafkaBaseTopicName() {
		return toKafkaBaseTopicName;
	}

	public void setToKafkaBaseTopicName(String toKafkaBaseTopicName) {
		this.toKafkaBaseTopicName = toKafkaBaseTopicName;
	}

	public String getQciDefaultValue() {
		return qciDefaultValue;
	}

	public void setQciDefaultValue(String qciDefaultValue) {
		this.qciDefaultValue = qciDefaultValue;
	}

	public String getAnchorDefaultValue() {
		return anchorDefaultValue;
	}

	public void setAnchorDefaultValue(String anchorDefaultValue) {
		this.anchorDefaultValue = anchorDefaultValue;
	}

	public String getForceNumber2Process() {
		return forceNumber2Process;
	}

	public void setForceNumber2Process(String forceNumber2Process) {
		this.forceNumber2Process = forceNumber2Process;
	}
	
}