package edu.doc_ti.jfcp.selec_reproc.interceptor.model;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import edu.doc_ti.jfcp.selec_reproc.interceptor.utils.Constants;

public class CDRRecord {
	
	private static final Logger LOG = LoggerFactory.getLogger(CDRRecord.class);
	
	private byte[] body;
	private Map<String, String> headers;
	
	
	public CDRRecord(){}
	public CDRRecord(byte[] body, Map<String, String> headers) {
		this.body = body;
		this.headers = headers;
	}
		
	public byte[] getBody() {
		return body;
	}
	
	public void setBody(byte[] body) {
		this.body = body;
	}
	
	public Map<String, String> getHeaders() {
		return headers;
	}
	
	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}
	
	

	/**
	 * Append string to body component
	 * @param currentBody
	 * @param stringToAppend
	 * @return
	 */
	public static byte[] operateWithCDRBody(byte[] currentBody, String stringToAppend) {
		byte[] enrichmentBody = currentBody;
		StringBuilder bodybuilder = new StringBuilder(new String(currentBody));
		
		LOG.debug("Append to body: {}", stringToAppend);
		bodybuilder.append(stringToAppend);
		enrichmentBody = bodybuilder.toString().getBytes(Charset.forName("UTF8"));
		
		return enrichmentBody;
	}

	public void modifyHeaders(String headerName, String newHeader) {
		LOG.debug("Current event headers: {}", getHeaders().toString());
		
		addHeader(headerName, newHeader);

		LOG.debug("New event headers: {}", getHeaders().toString());
	}
	
	/**
	 * Put header in event
	 * @param nameHeader
	 * @param value
	 * @return
	 */
	private void addHeader(String nameHeader, String value){
		if (!Strings.isNullOrEmpty(nameHeader) && !Strings.isNullOrEmpty(value)){
			LOG.debug("Put {} header with value=[{}]", nameHeader, value);
			getHeaders().put(nameHeader, value);
		}
	}
	
	/**
	 * Add string to cdr body
	 * @param stringToAppend
	 * @param delimiter 
	 */
	public void modifyBody(String stringToAppend, String delimiter) {
		StringJoiner joiner = new StringJoiner(delimiter);
		LOG.debug("Append to body: {}", stringToAppend);
				
		joiner.add(stringToAppend).add(new String(getBody()));
		String joinedString = joiner.toString();
		setBody(joinedString.getBytes(Charset.forName("UTF8")));
		LOG.debug("New body event: {}", joinedString);		
	}
	
	/**
	 * Get topic by cdr type
	 * @param delimiter
	 * @param cdrDatos
	 * @param cdrVoz
	 * @return
	 */
	public String getTopic(String delimiter, String cdrDatos, String cdrToDelete, String cdrVoz) {
		String cdrType= null;
		String sBody = new String(getBody());
		LOG.debug("Splitting event data to get CDR type: {}", sBody);
		
		if (!Strings.isNullOrEmpty(sBody)){
			
			String[] dataObjects = sBody.split(delimiter,-1);
			String fieldCdr = dataObjects[0];
			LOG.debug("CDR code from body: {}", fieldCdr);
			
			if (cdrDatos != null){
				String[] cdrsArray = cdrDatos.split(",");
				if (ArrayUtils.contains(cdrsArray, fieldCdr)){
					cdrType = Constants.CDRDATA;
				} else {
					cdrsArray = cdrToDelete.split(",");
					if (!ArrayUtils.contains(cdrsArray, fieldCdr)){
						cdrType = "invalid";
					}
				}
			} else if (cdrVoz != null){
				String[] cdrsArray = cdrVoz.split(",");
				if (ArrayUtils.contains(cdrsArray, fieldCdr)){
					cdrType = Constants.CDRVOICE;
				} else {
					cdrType = "invalid";
				}
			} else {
				LOG.warn("CDRs types not configures to this event: {}", sBody);
			}
		}
		LOG.debug("CDR type to apply: {}", cdrType);
		return cdrType;
	}
}
