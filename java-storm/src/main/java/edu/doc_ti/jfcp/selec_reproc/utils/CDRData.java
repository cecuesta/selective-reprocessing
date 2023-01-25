package edu.doc_ti.jfcp.selec_reproc.utils;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;



public class CDRData {

	Map<String, Object> cdrData = new HashMap<String, Object>();
	
	private static Logger logger = LoggerFactory.getLogger(CDRData.class);
	
	public String[] splitData(String data){
		String[] dataObjects = null;
		logger.debug("Splitting tuple data: {}", data);
		if (!Strings.isNullOrEmpty(data)){
			dataObjects = data.split(Constants.DELIMITER,-1);
		}
		return dataObjects;
	}
	public Map<String, Object> getCdrData() {
		return cdrData;
	}
	
	
	public CDRData(String rawData) {
		super();
		
    	String[] dataArray = splitData(rawData.toString());

		
		getCdrData().put(Constants.PROCESSINGDATE, System.currentTimeMillis() ) ; 
		getCdrData().put(Constants.TIMESTAMP, "");

		
		for ( int index = 0 ; index < dataArray.length; index++) {
			String val =  dataArray[index] ;
			if ( val != null && val.length() > 0 ) {
				getCdrData().put(FieldManager.fieldNames.get(index), val) ;
			}
			
		}

		
	}
}
