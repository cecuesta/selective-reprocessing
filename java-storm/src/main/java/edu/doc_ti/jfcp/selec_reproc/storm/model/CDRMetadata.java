package edu.doc_ti.jfcp.selec_reproc.storm.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants;
import edu.doc_ti.jfcp.selec_reproc.storm.utils.ValidatorDataType;


public class CDRMetadata implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7845891091462502313L;
	
	private static Logger LOG = LoggerFactory.getLogger(CDRMetadata.class);
	
	public SimpleDateFormat sdfKafkaDates = setSdfKafkaDates();

	private SimpleDateFormat sdfIndex = setSdfIndex();
	private SimpleDateFormat sdfTopic = setSdfTopic();

	public SimpleDateFormat sdfOutES = setSdfOutES();
	public SimpleDateFormat sdfVoice = setSdfVoice();
	public SimpleDateFormat sdfData = setSdfData();
	public SimpleDateFormat sdfDay= setSdfDay();
	public SimpleDateFormat sdfHour = setSdfHour();
	
	private HashMap<Integer,CDRField> propsDatoFieldManager = new HashMap<Integer,CDRField>();
	private HashMap<Integer,CDRField> propsVozFieldManager = new HashMap<Integer,CDRField>();
	private HashMap<String, CDRType> propsTypeManager = new HashMap<String,CDRType>();
	
	private ObjectMapper objMapper = new ObjectMapper();
	
	private List<String> notPrintDataFields = null;
	private List<String> notPrintVoiceFields = null;
	
	private ValidatorDataType validatorData = new ValidatorDataType();
	
	private Long processFromKafkaDate = new Long(0);
	private Long processToKafkaDate = new Long(0);
	
	public CDRMetadata() {}

	private SimpleDateFormat setSdfDay() {
		sdfDay = new SimpleDateFormat(Constants.DATEFORMAT_DAY);
		sdfDay.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfDay;
	}
	
	public SimpleDateFormat getSdfDay() {
		return sdfDay;
	}
	
	private SimpleDateFormat setSdfHour() {
		sdfHour = new SimpleDateFormat(Constants.DATEFORMAT_HOUR);
		sdfHour.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfHour;
	}
	
	public SimpleDateFormat getSdfHour() {
		return sdfHour;
	}
	
	private SimpleDateFormat setSdfKafkaDates() {
		sdfKafkaDates = new SimpleDateFormat(Constants.DATEFORMAT_KAFKADATES);
		sdfKafkaDates.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfKafkaDates;
	}
	
	public SimpleDateFormat getSdfKafkaDates() {
		return sdfKafkaDates;
	}

	public SimpleDateFormat setSdfIndex() {
		sdfIndex = new SimpleDateFormat(Constants.DATEFORMAT_TO_INDEXELK);
		sdfIndex.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfIndex;
	}
	
	public SimpleDateFormat getSdfIndex() {		
		return sdfIndex;
	}

	public SimpleDateFormat setSdfTopic() {
		sdfTopic = new SimpleDateFormat(Constants.DATEFORMAT_TO_KAFKA);
		sdfTopic.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfTopic;
	}
	public SimpleDateFormat getSdfTopic() {
		return sdfTopic;
	}

	public SimpleDateFormat setSdfOutES() {
		sdfOutES = new SimpleDateFormat(Constants.DATEFORMAT_DATEELK);
		sdfOutES.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfOutES;
	}
	public SimpleDateFormat getSdfOutES() {
		return sdfOutES;
	}

	public SimpleDateFormat setSdfVoice() {
		sdfVoice = new SimpleDateFormat(Constants.DATEFORMAT_VOICEFIELDS);
		//sdfVoice.setLenient(false);
		sdfVoice.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfVoice;
	}
	public SimpleDateFormat getSdfVoice() {
		return sdfVoice;
	}

	public SimpleDateFormat setSdfData() {
		sdfData = new SimpleDateFormat(Constants.DATEFORMAT_DATAFIELDS);
		//sdfData.setLenient(false);
		sdfData.setTimeZone(TimeZone.getTimeZone("CET"));
		return sdfData;
	}
	public SimpleDateFormat getSdfData() {
		return sdfData;
	}
	

	public HashMap<Integer, CDRField> getPropsDatoFieldManager() {
		return propsDatoFieldManager;
	}

	public void setPropsDatoFieldManager(HashMap<Integer, CDRField> propsDatoFieldManager) {
		this.propsDatoFieldManager = propsDatoFieldManager;
	}
	
	public HashMap<Integer, CDRField> getPropsVozFieldManager() {
		return propsVozFieldManager;
	}

	public void setPropsVozFieldManager(HashMap<Integer, CDRField> propsVozFieldManager) {
		this.propsVozFieldManager = propsVozFieldManager;
	}

	public HashMap<String, CDRType> getPropsTypeManager() {
		return propsTypeManager;
	}

	public void setPropsTypeManager(HashMap<String, CDRType> propsTypeManager) {
		this.propsTypeManager = propsTypeManager;
	}

	public ObjectMapper getObjMapper() {
		return objMapper;
	}

	public void setObjMapper(ObjectMapper objMapper) {
		this.objMapper = objMapper;
	}

	
	public List<String> getNotPrintDataFields() {
		return notPrintDataFields;
	}

	public void setNotPrintDataFields(List<String> notPrintDataFields) {
		this.notPrintDataFields = notPrintDataFields;
	}
	
	public List<String> getNotPrintVoiceFields() {
		return notPrintVoiceFields;
	}

	public void setNotPrintVoiceFields(List<String> notPrintVoiceFields) {
		this.notPrintVoiceFields = notPrintVoiceFields;
	}


	@SuppressWarnings("rawtypes")
	public void constructFieldsManager(int numberCdrFields, Map properties, String type) {
		for (int position=0; position < numberCdrFields; position++) {		
			String cdrFieldNameStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME +
					type + "." + position + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_ENDNAME;
			String cdrFieldName = (String)properties.get(cdrFieldNameStr);			
			
			if (!Strings.isNullOrEmpty(cdrFieldName)){
				
				String cdrSpecialFieldsStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME +
						type + "." + position + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_CALCULATED;
				String cdrSpecialFields = (String)properties.get(cdrSpecialFieldsStr);
				
				String cdrTypeFieldsStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME +
						type + "." + position + edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_TYPE;
				String cdrTypeFields = (String)properties.get(cdrTypeFieldsStr);
				
				List<String> listCdrCalculatedFields = null;
				if (!Strings.isNullOrEmpty(cdrSpecialFields)){
					listCdrCalculatedFields = Arrays.asList(cdrSpecialFields.split("\\s*,\\s*"));
				}
				
				if (type.equals(Constants.CDRS_FIELD_DATO)){
					propsDatoFieldManager.put(position, new CDRField(cdrFieldName,listCdrCalculatedFields,cdrTypeFields));
				} else {
					propsVozFieldManager.put(position, new CDRField(cdrFieldName,listCdrCalculatedFields,cdrTypeFields));
				}
			} else {
				String msg = cdrFieldName + " is not defined";
				System.err.println(msg) ;
				LOG.error(msg);
				System.exit(-1) ;
			}
		}
	}

		
	@SuppressWarnings("rawtypes")
	public void constructTypesManager(String listCdrTypesStr, Map cdrProperties) {

		for ( String key : (listCdrTypesStr).split(",") ) {
			key = key.trim() ;

			String cdrTypeNameStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME + key + 
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_ENDNAME;
			String cdrTypeName = (String) cdrProperties.get(cdrTypeNameStr) ;	
			
			String cdrTypePosStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME + key +
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_TYPE_ENDPOS;
			String cdrTypePos = (String)cdrProperties.get(cdrTypePosStr);
			
			String cdrTypePosKeyStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME + key +
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_TYPE_ENDKEY;
			String cdrTypePosKey = (String)cdrProperties.get(cdrTypePosKeyStr);
			
			String cdrTypeStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME + key +
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_TYPE_ENDTYPE;
			String cdrType = (String)cdrProperties.get(cdrTypeStr);
			
			String cdrTypeFlowStr = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME + key +
					edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_TYPE_ENDFLOW;
			String cdrTypeFlow = (String)cdrProperties.get(cdrTypeFlowStr);
			
			if (!Strings.isNullOrEmpty(cdrTypeName) && !Strings.isNullOrEmpty(cdrTypePos) &&
					!Strings.isNullOrEmpty(cdrTypePosKey) && !Strings.isNullOrEmpty(cdrType)){
				
				List<Integer> listCdrTypePos = null;
				if (!Strings.isNullOrEmpty(cdrTypePos)){
					listCdrTypePos = new ArrayList<>();
					String [] positionsRaw = cdrTypePos.split("\\s*,\\s*");
					try {
						for(int i = 0; i < positionsRaw.length;i++){
					        if(positionsRaw[i].contains("-")){
					            String[] posRange = positionsRaw[i].split("-");
					            int from =  Integer.parseInt(posRange[0]);
					            int to = Integer.parseInt(posRange[1]);
					            for(int value = from; value <= to; value++){
					            	listCdrTypePos.add(value);
					            }
					        }
					        else {
					        	listCdrTypePos.add(Integer.valueOf(positionsRaw[i]));
					        }
					    }
						
						propsTypeManager.put(cdrTypeName, new CDRType(Integer.valueOf(cdrTypePosKey),listCdrTypePos,cdrType,cdrTypeFlow));
						LOG.debug("TypesManager Properties=[key={},positionData={},positions={},type={},flow={}]",cdrTypeName,Integer.valueOf(cdrTypePosKey),listCdrTypePos.toArray(),cdrType,cdrTypeFlow);
						
					} catch (NumberFormatException nfe){
						String msg = "TypesManager Invalid position cdr type=" +  cdrTypePos + " !!!!!!";
						System.err.println(msg) ;
						LOG.error(msg);
						System.exit(-1) ;
					}
				}
			} else {
				String msg =  edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_INITBASENAME + key + " is not defined properly";
				System.err.println(msg) ;
				LOG.error(msg);
				System.exit(-1) ;
			}
		}
	}

	/**
	 * Get fields list not present in output
	 * @param type
	 * @param cdrsNotPrintStr 
	 */
	public void constructListNotPrint(String type, String cdrsNotPrintStr) {
		if (edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.CDRS_FIELD_DATO.equals(type)){
			setNotPrintDataFields(new ArrayList<>());
			String [] fields = cdrsNotPrintStr.split("\\s*,\\s*");
			setNotPrintDataFields(Arrays.asList(fields));
		} else {
			setNotPrintVoiceFields(new ArrayList<>());
			String [] fields = cdrsNotPrintStr.split("\\s*,\\s*");
			setNotPrintVoiceFields(Arrays.asList(fields));
		}
	}

	public ValidatorDataType getValidatorData() {
		return validatorData;
	}

	public void setValidatorData(ValidatorDataType validatorData) {
		this.validatorData = validatorData;
	}

	public Long getProcessFromKafkaDate() {
		return processFromKafkaDate;
	}

	public void setProcessFromKafkaDate(Long processFromKafkaDate) {
		this.processFromKafkaDate = processFromKafkaDate;
	}

	public Long getProcessToKafkaDate() {
		return processToKafkaDate;
	}

	public void setProcessToKafkaDate(Long processToKafkaDate) {
		this.processToKafkaDate = processToKafkaDate;
	}

}
