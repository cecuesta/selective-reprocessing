package edu.doc_ti.jfcp.selec_reproc.storm.bolt;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */



import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.doc_ti.jfcp.selec_reproc.utils.CDRData;

public class ProcessBolt extends BaseRichBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 689163422885984769L;

	protected static final Logger LOG = LoggerFactory.getLogger(ProcessBolt.class);
    private OutputCollector collector;
    private ObjectMapper mapper  ;
    @SuppressWarnings("unused")
	private TypeReference<Map<String, String>> typeRef  ;

    
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
		mapper = new ObjectMapper();
		typeRef = new TypeReference<Map<String, String>>(){} ;    }

    long newLogAt = System.currentTimeMillis() ;
    
    
    long count = 0 ; 
    @Override
    public void execute(Tuple tuple) {

    	boolean inLog = System.currentTimeMillis() >= newLogAt ;
    	if ( inLog ) {
    		LOG.info("input = [" + tuple + "]");
    		newLogAt+= 5000 ;
    	}

    	
    	try {
    		
    		count++ ;
    		CDRData cdr = new CDRData( tuple.getValue(0).toString() ) ;
	        String jsonData = "{ \"campo1\"  : \"dato1\", \"ts\" : " + System.currentTimeMillis() + " }" ;

	        jsonData = mapper.writeValueAsString(cdr.getCdrData()) ;

	        if ( inLog ) {
	        	LOG.info("hashMap: " + cdr.getCdrData() );
	        	LOG.info("json: " + jsonData);
	        	LOG.info("Records processed: " + count + " ---------------- ");
	        }
	        
			collector.emit(tuple ,  new Values("index-data", jsonData) ) ;
    	} catch (Exception ex) {
    		ex.printStackTrace(); 
    	}
        
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fObj = new Fields("index", "json")	 ;	
		declarer.declare(fObj);

    }
    
    
    
}