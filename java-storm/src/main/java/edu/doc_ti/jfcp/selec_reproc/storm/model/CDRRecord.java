package edu.doc_ti.jfcp.selec_reproc.storm.model;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.FilenameUtils;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.doc_ti.jfcp.selec_reproc.storm.utils.CasesManager;
import edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants;

public class CDRRecord {

	private static Logger LOG = LoggerFactory.getLogger(CDRRecord.class);
	
	private Object receivedData;
	private CDRMetadata metadata;
	private CasesManager casesManager;
	private String index;
	private String docTypeName;
	private String toKafkaTopic;
	private static String qciDefaultValue;
	private static String anchorDefaultValue;
	private static String forceNumber2Process;
	private long timestamp = 0;
	private String type;
	Map<String, Object> cdrData = new HashMap<String, Object>();
	
	private String file;
	private String numLinea;

	private final static String separator4Hive = "|";
	
	public CDRRecord() {}
	
	public CDRRecord( String indexBaseName, String docTypeName, String toKafkaTopic, String qciDefaultValue, String anchorDefaultValue, String forceNumber2Process, CDRMetadata metadata, CasesManager casesManager) {
		this.setIndex(indexBaseName);
		this.setDocTypeName(docTypeName);
		this.setToKafkaTopic(toKafkaTopic);
		this.setQciDefaultValue(qciDefaultValue);
		this.setAnchorDefaultValue(anchorDefaultValue);
		this.setForceNumber2Process(forceNumber2Process);
		this.setMetadata(metadata);
		this.setCasesManager(casesManager);
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public Object getReceivedData() {
		return receivedData;
	}

	public void setReceivedData(Object receivedData) {
		this.receivedData = receivedData;
	}
	
	public CDRMetadata getMetadata() {
		return metadata;
	}

	public void setMetadata(CDRMetadata metadata) {
		this.metadata = metadata;
	}
	
	public CasesManager getCasesManager() {
		return casesManager;
	}

	public void setCasesManager(CasesManager casesManager) {
		this.casesManager = casesManager;
	}

	public String getDocTypeName() {
		return docTypeName;
	}

	public void setDocTypeName(String docTypeName) {
		this.docTypeName = docTypeName;
	}

	public String getToKafkaTopic() {
		return toKafkaTopic;
	}

	public void setToKafkaTopic(String toKafkaTopic) {
		this.toKafkaTopic = toKafkaTopic;
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
	
	public static String getForceNumber2Process() {
		return forceNumber2Process;
	}

	public static void setForceNumber2Process(String forceNumber2Process) {
		CDRRecord.forceNumber2Process = forceNumber2Process;
	}

	public long getTimestamp() {
		if (timestamp == 0){
			timestamp = System.currentTimeMillis();
		}
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public String getNumLinea() {
		return numLinea;
	}

	public void setNumLinea(String numLinea) {
		this.numLinea = numLinea;
	}

	public Map<String, Object> getCdrData() {
		return cdrData;
	}

	public void setCdrData(Map<String, Object> cdrData) {
		this.cdrData = cdrData;
	}	

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	/**
	 * Create object to redirect to next elements
	 * @param rawData
	 */
	public void setObjectToRedirect(Object rawData){
	    if (rawData != null){
	    	String[] dataArray = getMetadata().getValidatorData().splitData(rawData.toString());
	    	setFile(FilenameUtils.getName(dataArray[0]));
    		setNumLinea(dataArray[1]);
    		
			getCdrData().put(Constants.PROCESSINGDATE, getMetadata().getValidatorData().validateDate(getMetadata().getSdfKafkaDates(), getTimestamp()));
			getCdrData().put(Constants.TIMESTAMP, getMetadata().getValidatorData().validateDate(getMetadata().getSdfOutES(), getTimestamp()));
			getCdrData().put(Constants.FILENAME, getFile());
			getCdrData().put(Constants.POSITION, getNumLinea());
	    	
			boolean incorrectData = false;
			
			String cdr_key =  null;
			List<Integer> positions = null;
			if (!getMetadata().getPropsTypeManager().isEmpty()){
				String flow = null;
				
				for (Map.Entry<String,CDRType> entry : getMetadata().getPropsTypeManager().entrySet()) {
			        String key = entry.getKey();
			        CDRType value = entry.getValue();
			        			        
			        int posData = value.getPositionData();
			        if (dataArray[posData].equals(key)){
			        	positions = value.getPositions();
			        	type = value.getType();
				        flow = value.getFlow();
				        cdr_key = key;
			        }
			    }
				
				int dataLength = dataArray.length;
				HashMap<Integer,CDRField> propsFieldManager = null;
				if (Constants.CDRS_FIELD_DATO.equals(type)){
					getCdrData().put(Constants.DATAFLOW, flow);
					getCdrData().put("end_of_record", "#"); 
					propsFieldManager = getMetadata().getPropsDatoFieldManager();
				} else if (Constants.CDRS_FIELD_VOZ.equals(type)) {
					getCdrData().put(Constants.CALLFLOW, flow);
					getCdrData().put("end_of_record", "#");
					propsFieldManager = getMetadata().getPropsVozFieldManager();	
					dataLength -= 1; //Esto es porque al final del �ltimo campo aparece el car�cter �,� y el array lo toma como campo vac�o
				} else {
					incorrectData = true;
					LOG.warn("[{}:{}] - CDR type is not configured. This record will be not processed !!!!!!", getFile(), getNumLinea());
				}
				
				// Es un tipo de CDR configurado como dato o voz
				if (!incorrectData){
					
					int positionsLength = positions.size();	
					String forceNumberProcess = null;
					
					// Se forzar� el procesado de campos superior a los configurados a partir del valor "forceNumber2Process" cuando �ste tenga valor (en caso contrario, por defecto, se insertar� 0)
					if ((dataLength == positionsLength)) {
						forceNumberProcess = "0";
					}else {
						if (getForceNumber2Process().contains("-")) {
							for (String number2ProcessByType : getForceNumber2Process().split(",")) {
								if (number2ProcessByType.contains(cdr_key+"-")) {
									forceNumberProcess = number2ProcessByType.split("-")[1];
								}
							}
						}
					}
					if (forceNumberProcess == null) {
						forceNumberProcess = "0";
					}
					// Num. de campos del CDR coincide con el num. de campos configurados
					if (dataLength == positionsLength || (Integer.parseInt(forceNumberProcess) > 0 && dataLength == Integer.parseInt(forceNumberProcess))){
						
						positionsLength = (Integer.parseInt(forceNumberProcess) > 0 && positionsLength > Integer.parseInt(forceNumberProcess)) ? dataLength : positionsLength;
						boolean processTuple = true;
						
						for (int position = 0; position < positionsLength && processTuple; position++){
							String dataToProcess = dataArray[position];
							
							// Se inserta campo en JSON si el valor es NO nulo
							if (!Strings.isNullOrEmpty(dataToProcess)){
								
								int cdrFieldPosition = positions.get(position);
								edu.doc_ti.jfcp.selec_reproc.storm.model.CDRField cdrObject = propsFieldManager.get(cdrFieldPosition);
								
								if (cdrObject != null){
									String fieldTOJson = cdrObject.getName();
									
									// Insertamos campo y valor en JSON
									if (cdrObject.getType() == null){
										getCdrData().put(fieldTOJson, dataToProcess);
									} else {
										String typeField = cdrObject.getType().toLowerCase();
										Object objToELK = getMetadata().getValidatorData().convertTypeObject(typeField, dataToProcess);
										getCdrData().put(fieldTOJson, objToELK);
									}
									
									// Comprobamos si es campo sobre el que realizar operaciones
									if (cdrObject.getCalculatedFields() != null){
										for (String calculatedField : cdrObject.getCalculatedFields()){
											String valueToELK = null;
											
											try {
												valueToELK = manageSpecialFieldsToELK(fieldTOJson, calculatedField, dataToProcess);
											} catch (Exception e){
												valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
											}
											
											// El valor calculado es NO nulo y, por tanto, se inserta en el JSON
											if (valueToELK != null){
												if (valueToELK.equals(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.NOPROCESS_VALUE)){
													processTuple = false;
													getCdrData().put(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.NOPROCESS_VALUE, edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.NOPROCESS_VALUE);
//													LOG.warn("[{}:{}] - CDR is not in a valid dates range to process it ...", getFile(), getNumLinea());
												} else {
													if (valueToELK.equals(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE)){
														incorrectData = true;
														LOG.warn("[{}:{}] - CDR field {} with invalid value (origin value={}). "
																+ "CDR will be not processed !!!!!!", getFile(), getNumLinea(), calculatedField, dataToProcess);
													} else {
														getCdrData().put(calculatedField, valueToELK);
													}
												}
											}
										}
									}
									LOG.debug("[{}:{}] - Add pair key/value to Json Object: [{}]=[{}]", getFile(), getNumLinea(), cdrObject.getName(), dataToProcess);			    				
								}
							}
						}
					} else {
						// Num. de campos del CDR NO coincide con el num. de campos configurados
						incorrectData = true;
						if (dataLength > positionsLength){
							LOG.warn("[{}:{}] - Incorrect data. More fields respect configuration properties (Number of fields in properties= "+ positionsLength + " ;and number of data= " + dataLength + "): [{}]", getFile(), getNumLinea(), rawData.toString());
						} else {
							LOG.warn("[{}:{}] - Incorrect data. Missing fields respect configuration properties: [{}]", getFile(), getNumLinea(), rawData.toString());
						}
					}
				}	
			} else {
				incorrectData = true;
				LOG.warn("CDRs types are not configured. CDR will be not processed: [{}]!!!!!!", rawData.toString());
			}
			
			if (incorrectData){
				getCdrData().put(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR, edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE);
			}
	    }
	}



	private boolean validateDateToProcessTuple(long millisecondsFromData) {
		boolean process = true;
		
		if (getMetadata().getProcessFromKafkaDate() != 0){
			if (millisecondsFromData < getMetadata().getProcessFromKafkaDate()){	
				process = false;
			}
		}
		
		if (process && getMetadata().getProcessToKafkaDate() != 0){
			if (millisecondsFromData > getMetadata().getProcessToKafkaDate()){
				process = false;
			}
		}
		
		return process;
	}
	

	/**
	 * Apply enrichment to JSON Object
	 */
	public void enrichObjectToRedirect() {

	}
	
	
	private void manageQci(Map<String, Object> cdrData2) {
		LOG.debug("------Creating QCI field without valid value------: ");
		cdrData.put(Constants.QCI, getQciDefaultValue());
	}
	
	private void manageNodeSession(Map<String, Object> cdrData2) {
		if (cdrData.get(Constants.NODE_SESSION) == null || cdrData.get(Constants.NODE_SESSION).toString().isEmpty()) {
			LOG.debug("------Changing Pos 3 value with Pos 34------: ");
			cdrData.put(Constants.NODE_SESSION, cdrData.get(Constants.GGSN_ADDR));
		}
	}

	private String manageSpecialFieldsToELK(String fieldTOJson, String calculatedField, String dataToProcess) throws Exception {
		
		String valueToELK = null;
		boolean error = false;
		boolean processTuple = true; 
		
		if (fieldTOJson.equalsIgnoreCase(Constants.HOUR)){
			String fullDate = getMetadata().getValidatorData().getVoiceDateHour((String)getCdrData().get(Constants.DAY), dataToProcess);
			long ms = getMetadata().getValidatorData().getVoiceMilliseconds(getMetadata().getSdfVoice(),fullDate);
			
			if (ms > 0){
				if (calculatedField.equalsIgnoreCase(Constants.TIMESTAMP) || 
						calculatedField.equalsIgnoreCase(Constants.INITDATE)){
				
					if (calculatedField.equalsIgnoreCase(Constants.TIMESTAMP)){
						setTimestamp(ms);
						processTuple = validateDateToProcessTuple(ms);
					}
					valueToELK = getMetadata().getValidatorData().validateDate(getMetadata().getSdfOutES(), ms);
				} else if (calculatedField.equalsIgnoreCase(Constants.DAYHOUR)){
					valueToELK = fullDate;
				} else {
					error = true;
				}
			} else {
				error = true;
			}
			
			
			if (processTuple){
				if (error){
					valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
				}
			} else {
				valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.NOPROCESS_VALUE;
			}
			
		} else if (fieldTOJson.equalsIgnoreCase(Constants.INITDATE) || 
			fieldTOJson.equalsIgnoreCase(Constants.ENDDATE)) {
			long ms = getMetadata().getValidatorData().getDataMilliseconds(getMetadata().getSdfData(),dataToProcess);
			if (ms > 0){
				if (calculatedField.equalsIgnoreCase(Constants.TIMESTAMP) ||
						calculatedField.equalsIgnoreCase(Constants.INITDATE) || 
						calculatedField.equalsIgnoreCase(Constants.ENDDATE)){
					
					if (calculatedField.equalsIgnoreCase(Constants.TIMESTAMP)){
						setTimestamp(ms);
						processTuple = validateDateToProcessTuple(ms);
					}
					valueToELK = getMetadata().getValidatorData().validateDate(getMetadata().getSdfOutES(), ms);
				} else if (calculatedField.equalsIgnoreCase(Constants.INITDATE_DAY) || calculatedField.equalsIgnoreCase(Constants.ENDDATE_DAY)){
					valueToELK = getMetadata().getValidatorData().validateDate(getMetadata().getSdfDay(), ms);
				} else if (calculatedField.equalsIgnoreCase(Constants.INITDATE_HOUR) || calculatedField.equalsIgnoreCase(Constants.ENDDATE_HOUR)){
					valueToELK = getMetadata().getValidatorData().validateDate(getMetadata().getSdfHour(), ms);
				} else if (calculatedField.equalsIgnoreCase(Constants.INITDATE_DAYHOUR)){					
					valueToELK = (String)getCdrData().get(Constants.INITDATE_DAY) + (String)getCdrData().get(Constants.INITDATE_HOUR);
				} else if (calculatedField.equalsIgnoreCase(Constants.ENDDATE_DAYHOUR)) {
					valueToELK = (String)getCdrData().get(Constants.ENDDATE_DAY) + (String)getCdrData().get(Constants.ENDDATE_HOUR);
				} else {
					error = true;
				}				
			} else {
				error = true;
			}
			
			if (processTuple){
				if (error){
					valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
				}
			} else {
				valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.NOPROCESS_VALUE;
			}
			
		} else {
			valueToELK = getCasesManager().manageSpecialFieldsToELK(getFile(), getNumLinea(), getCdrData(), fieldTOJson, calculatedField, dataToProcess);
		}

		return valueToELK;
		
	}

	
	/**
	 * Create JSON string to redirect
	 * @return
	 */
	public String getJsonStrToELK (){
		try {
			return getMetadata().getObjMapper().writeValueAsString(getCdrData());
		} catch (JsonProcessingException e) {
			return "";
		}
	}
	


	
	/**
	 * Change values Post-ELK
	 */
	private void manageFieldsPostELK() {
		casesManager.manageFieldsPostELK(getCdrData());
		
		List<String> notPrintList = null;
		if (Constants.CDRS_FIELD_DATO.equals(type)){
			notPrintList = getMetadata().getNotPrintDataFields();
		} else {
			notPrintList = getMetadata().getNotPrintVoiceFields();
		}
			
		if (notPrintList != null){
			for (String notPrintField : notPrintList){
				if (getCdrData().containsKey(notPrintField)){
					getCdrData().remove(notPrintField);
				}
			}
		}
		
		if (Constants.CDRS_FIELD_DATO.equals(type)){
			if (cdrData.get(Constants.CELL) != null) {
				manageAnchor(Constants.CELL_ANCHOR, Constants.CELL);
			}
		}

		if (Constants.CDRS_FIELD_VOZ.equals(type)){
			if (cdrData.get(Constants.CELL_INI) != null) {
				manageAnchor(Constants.CELL_INI_ANCHOR, Constants.CELL_INI);
			}
			
			if (cdrData.get(Constants.CELL_END) != null) {
				manageAnchor(Constants.CELL_END_ANCHOR, Constants.CELL_END);
			}
		}
		
		manage5GFields();
//		LOG.debug("[{}:{}] - Store cdr: {}", getFile(), getNumLinea(), Arrays.asList(getCdrData()));
	}
	
	private void manage5GFields() {
		if(StringUtils.isNotEmpty((String) cdrData.get(Constants.NSA_UPLOAD_VOLUME)))
			cdrData.put(Constants.NSA_UPLOAD_VOLUME, Long.parseLong((String) cdrData.get(Constants.NSA_UPLOAD_VOLUME)));
		if(StringUtils.isNotEmpty((String) cdrData.get(Constants.NSA_DOWNLOAD_VOLUME)))
			cdrData.put(Constants.NSA_DOWNLOAD_VOLUME, Long.parseLong((String) cdrData.get(Constants.NSA_DOWNLOAD_VOLUME)));
		if(StringUtils.isNotEmpty((String) cdrData.get(Constants.NSA_DURATION)))
			cdrData.put(Constants.NSA_DURATION, Long.parseLong((String) cdrData.get(Constants.NSA_DURATION)));
		
		if(StringUtils.isNotEmpty((String) cdrData.get(Constants.NSA_START_TIME)))
			cdrData.put(Constants.NSA_START_TIME, getMetadata().getValidatorData().validateDate(getMetadata().getSdfOutES(), getTimestamp()));
		
//		getMetadata().getValidatorData().validateDate(getMetadata().getSdfOutES(), Timestamp.valueOf((String) cdrData.get(Constants.NSA_START_TIME)).getTime());
	}

	private void manageAnchor(String anchorfield, String cellField) {
		LOG.debug("------Creating ANCHOR field without valid value------: ");
		if(cdrData.get(anchorfield) != null && cdrData.get(cellField).toString().equalsIgnoreCase(cdrData.get(anchorfield).toString())) {
			cdrData.put(anchorfield, getAnchorDefaultValue());
		}
	}

	/**
	 * Set out kafka topic
	 */
	public void setTopicToRedirect() {
		SimpleDateFormat sdfTopic = getMetadata().getSdfTopic();
		String date = sdfTopic.format(new Date(Long.valueOf(getTimestamp())));
		String outTopic = getToKafkaTopic() + date;
		if (getCdrData().get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR) != null){
			outTopic = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INDEX_TOPIC_ERR + outTopic;
			LOG.debug("[{}:{}] - Received tuple with errors, send it to {}", getFile(), getNumLinea(), outTopic);
		}
		setToKafkaTopic(outTopic);
	}

	
	/**
	 * Set index to store the CDR in ELK
	 */
	public void setIndexToRedirect() {
		SimpleDateFormat sdfIndex = getMetadata().getSdfIndex();
		String date = sdfIndex.format(new Date(Long.valueOf(getTimestamp())));
	    String index = getIndex() + date;
		if (getCdrData().get(edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR) != null){
			index = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.INDEX_TOPIC_ERR + index;
			LOG.debug("[{}:{}] - Received tuple with errors, send it to {}", getFile(), getNumLinea(), index);
		}
		setIndex(index);
	}

	public static void main (String[] args) {

	}
	

}