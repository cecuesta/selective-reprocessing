package edu.doc_ti.jfcp.selec_reproc.storm.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class ValidatorDataType {
	
	private static Logger logger = LoggerFactory.getLogger(ValidatorDataType.class);

	public ValidatorDataType() { }
	

	/**
	 * Get parameters from string
	 * @param data
	 * @return
	 */
	public String[] splitData(String data){
		String[] dataObjects = null;
		logger.debug("Splitting tuple data: {}", data);
		if (!Strings.isNullOrEmpty(data)){
			dataObjects = data.split(Constants.DELIMITER,-1);
		}
		return dataObjects;
	}

	/**
	 * Validate date and format date
	 * @param sdfOutES 
	 * @param ms
	 * @return
	 */
	public String validateDate(SimpleDateFormat sdfOut, long ms){
		String valueString = null;
		if (ms != 0){
			Date dateWithTZ = new Date(ms);
			valueString = sdfOut.format(dateWithTZ);
		}	
		return valueString;
	}
	
	/**
	 * Get milliseconds from cdr-data value
	 * @param sdfData
	 * @param fecha
	 * @return
	 */
	public long getDataMilliseconds(SimpleDateFormat sdfData, String fecha) {
		long milliseconds = 0;

		try {
			Date date = sdfData.parse(fecha);
			milliseconds = date.getTime();			
		} catch (ParseException e) {}
		
		return milliseconds;
	}

	
	public String getVoiceDateHour(String fecha, String hour) {
		String fullDate = null;
		if (!Strings.isNullOrEmpty(fecha) && !Strings.isNullOrEmpty(hour)){
			fullDate = fecha + hour;		
		}
		return fullDate;
	}
	
	/**
	 * Get milliseconds from cdr-voz value
	 * @param sdfVoice
	 * @param fullDate
	 * @return
	 */
	public long getVoiceMilliseconds(SimpleDateFormat sdfVoice, String fullDate) {
		long milliseconds = 0;
		try {
			Date date = sdfVoice.parse(fullDate);
			milliseconds = date.getTime();
		} catch (ParseException e) {}
		return milliseconds;
	}
	
	/**
	 * Convert type to store in ELK
	 * @param typeField
	 * @param valueToconvert
	 * @return
	 */
	public Object convertTypeObject (String typeField, String valueToconvert){
		Object valueConverted = null;
		if (Constants.MAP_TYPEFIELDS.contains(typeField)){
			
			switch (typeField) {
			    case "long":
			    	try {
			    		valueConverted = Long.valueOf(valueToconvert);
			    	} catch (NumberFormatException nfe){
			    		valueConverted = valueToconvert;
			    	}
			        break;

			    case "integer":
			    	try {
			    		valueConverted = Integer.valueOf(valueToconvert);
			    	} catch (NumberFormatException nfe){
			    		valueConverted = valueToconvert;
			    	}
			        break;
			        
			    default:
			    	valueConverted = valueToconvert;
			}
		} else {
			valueConverted = valueToconvert;
		}
		return valueConverted;
	}

	
	public static void main (String[] args){
		String d1 = "171003140000+0200";
		ValidatorDataType vdt = new ValidatorDataType();
		long ms1 = vdt.getDataMilliseconds(new SimpleDateFormat(Constants.DATEFORMAT_DATAFIELDS), d1);
		String date1s = vdt.validateDate(new SimpleDateFormat(Constants.DATEFORMAT_DATEELK), ms1);
		System.out.println("dato:" + date1s);
		
		String d2 = "20171003140000";
		long ms2 = vdt.getDataMilliseconds(new SimpleDateFormat(Constants.DATEFORMAT_VOICEFIELDS), d2);
		String date2s = vdt.validateDate(new SimpleDateFormat(Constants.DATEFORMAT_DATEELK), ms2);
		System.out.println("voz:" + date2s);
	}

}
