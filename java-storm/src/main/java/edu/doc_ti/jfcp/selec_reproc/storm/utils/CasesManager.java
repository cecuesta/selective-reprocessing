package edu.doc_ti.jfcp.selec_reproc.storm.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CasesManager {

	private static Logger LOG = LoggerFactory.getLogger(CasesManager.class);

	/**
	 * Process special fields to ELK
	 * @param file
	 * @param numLinea
	 * @param cdrData
	 * @param metadata
	 * @param fieldTOJson
	 * @param calculatedField
	 * @param dataToProcess
	 * @return
	 * @throws Exception
	 */
	public String manageSpecialFieldsToELK(String file, String numLinea, Map<String, Object> cdrData, String fieldTOJson, String calculatedField, String dataToProcess) throws Exception {
		String valueToELK = null;
		
		if ((calculatedField.equalsIgnoreCase(Constants.HLRI) || calculatedField.equalsIgnoreCase(Constants.HLRF))){
			
			valueToELK = manageHLR(dataToProcess, calculatedField);
									
		} else if (calculatedField.equalsIgnoreCase(Constants.MCCMNC)){
			
			valueToELK = manageMCCMNC(dataToProcess);

		} else if (calculatedField.equalsIgnoreCase(Constants.IMEITAC)){
			
			valueToELK = manageTAC(dataToProcess);

		} else if (calculatedField.startsWith(Constants.NPXMSISDN)){
			
			valueToELK = manageMSISDN(dataToProcess);

		} else if (calculatedField.equalsIgnoreCase(Constants.POS_FALLO)){
			
			valueToELK = managePosFallo(dataToProcess);

		} else if (calculatedField.equalsIgnoreCase(Constants.CAUSA_INTERNA)){
			
			valueToELK = manageInternalCause(dataToProcess);

		} else if (fieldTOJson.equalsIgnoreCase(Constants.MSCNAME)) {
			
			valueToELK = manageMSC(dataToProcess);

		} else if (fieldTOJson.equalsIgnoreCase(Constants.ROUTEIN) || fieldTOJson.equalsIgnoreCase(Constants.ROUTEOUT)) {
			
			valueToELK = manageRoute(dataToProcess);

		} else if (fieldTOJson.equalsIgnoreCase(Constants.LOC_AREACODE)){
			
			valueToELK = manageAreaCode(dataToProcess);
			
		} else if (calculatedField.equalsIgnoreCase(Constants.CELL)){
			
			valueToELK = manageCell(cdrData, dataToProcess);

		} else if (calculatedField.equalsIgnoreCase(Constants.ROAMING_ZONE)) {
			if (cdrData.get(Constants.CELL) != null) {
				valueToELK = manageMCCMNC((String)cdrData.get(Constants.CELL));
			}
		} else if (calculatedField.equalsIgnoreCase(Constants.RAT_TYPE)) {
			valueToELK = dataToProcess;
		}

		LOG.debug("[{}:{}] - Add pair key/value to Json Object: [{}]=[{}]", file, numLinea, calculatedField, valueToELK);
		return valueToELK;
	}
	
	


	private String manageCell(Map<String, Object> cdrData, String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		if (dataToProcess.length() >= 15 || cdrData.get(Constants.LOC_AREACODE) == null) {
			valueToELK = dataToProcess;
		} else {				 
			 String aux_locAreaCode = null;
			 String locAreaCode = (String)cdrData.get(Constants.LOC_AREACODE);
			 if (locAreaCode != null){
				 if (locAreaCode.length() >= 5){
					 aux_locAreaCode = locAreaCode.substring(0, 5);
				 } else {
					 aux_locAreaCode = org.apache.commons.lang.StringUtils.leftPad(locAreaCode, 5, "0");
				 }
			 }
			 
			 String aux_cell = null;
			 if (dataToProcess.length() >= 5){
				 aux_cell = dataToProcess.substring(0, 5);
			 } else {
				 aux_cell = org.apache.commons.lang.StringUtils.leftPad(dataToProcess, 5, "0");
			 }
			 
			 if (aux_locAreaCode != null) {
				 valueToELK = Constants.IMSIBASEHLR + aux_locAreaCode + aux_cell ;
			 } else {
				 valueToELK = dataToProcess;
			 }
		}
		
		if (Constants.IMSIBASEHLR_SPECIAL_FLOW.equals(cdrData.get(Constants.DATAFLOW)) && 
				valueToELK.startsWith(Constants.IMSIBASEHLR_SPECIAL)){
			String lastPart= valueToELK.substring(Constants.IMSIBASEHLR_SPECIAL.length(),valueToELK.length());
			valueToELK = Constants.IMSIBASEHLR + lastPart;
		}			
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageAreaCode(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		if (dataToProcess.length() < 5) {
			valueToELK = org.apache.commons.lang.StringUtils.leftPad(dataToProcess, 5, "0");
		} else {
			valueToELK = dataToProcess;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageRoute(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		if (dataToProcess.length() > 0 &&
				(dataToProcess.endsWith(Constants.ROUTEIN_I) || dataToProcess.endsWith(Constants.ROUTEOUT_O))) {
			valueToELK = dataToProcess.substring(0, dataToProcess.length() - 1);
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageMSC(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		String[] arrayMsc = dataToProcess.split("\\s+");
		if (arrayMsc != null && arrayMsc.length > 1){
			valueToELK = arrayMsc[0];
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageInternalCause(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		try {
			valueToELK = dataToProcess;
			if (dataToProcess.length() > 2){
				String hexValue = dataToProcess.substring(2);
				valueToELK = String.valueOf(Integer.parseInt(hexValue, 16 ));
			}
		} catch (IndexOutOfBoundsException | NumberFormatException e) {
			error = true;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String managePosFallo(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		try {
			valueToELK = dataToProcess;
			if (dataToProcess.length() > 1){
				String hexValue = dataToProcess.substring(0,2);
				valueToELK = String.valueOf(Integer.parseInt(hexValue, 16 ));
			}	
			
		} catch (IndexOutOfBoundsException | NumberFormatException e) {
			error = true;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageMSISDN(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		try {
			if ((dataToProcess.length() == Constants.MSISDN_34 && dataToProcess.startsWith(Constants.MSISDN_34_VAL)) || 
				(dataToProcess.length() == Constants.MSISDN_0034 && dataToProcess.startsWith(Constants.MSISDN_0034_VAL))) {
				valueToELK = dataToProcess.substring(dataToProcess.length() - Constants.MSISDN);
			} else if ((dataToProcess.length() == Constants.MSISDN_3459 && dataToProcess.startsWith(Constants.MSISDN_3459_VAL)) || 
				(dataToProcess.length() == Constants.MSISDN_003459 && dataToProcess.startsWith(Constants.MSISDN_003459_VAL))) { 
				valueToELK = dataToProcess.substring(dataToProcess.length() - Constants.MSISDN_0034);
			} else {
				valueToELK = dataToProcess;
			}
		} catch (IndexOutOfBoundsException iobe) {
			error = true;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageTAC(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		try {
			valueToELK = dataToProcess;
			if (dataToProcess.length() >= Constants.IMEITAC_LENGTH){
				valueToELK = dataToProcess.substring(0,Constants.IMEITAC_LENGTH);
			}
		} catch (IndexOutOfBoundsException iobe) {
			error = true;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageMCCMNC(String dataToProcess) {
		String valueToELK = null;
		boolean error = false;
		
		try {
			valueToELK = dataToProcess;
			if (dataToProcess.length() >= Constants.MCCMNC_LENGTH){
				valueToELK = dataToProcess.substring(0,Constants.MCCMNC_LENGTH);
			}			
		} catch (IndexOutOfBoundsException iobe) {
			error = true;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}


	private String manageHLR(String dataToProcess, String calculatedField) {
		String valueToELK = null;
		boolean error = false;
		
		try {
			valueToELK = dataToProcess;
			
			if (calculatedField.startsWith(Constants.HLRI)){
				if (dataToProcess.length() >= Constants.MCCMNC_LENGTH+2){
					valueToELK = dataToProcess.substring(0,Constants.MCCMNC_LENGTH+2); //21403XY
				}
			} else {				
				if (dataToProcess.length() >= Constants.MCCMNC_LENGTH+3){
					valueToELK = dataToProcess.substring(0,Constants.MCCMNC_LENGTH+3); //21403XY[Z]						
				}
			}

		} catch (IndexOutOfBoundsException iobe) {
			error = true;
		}
		
		if (error){
			valueToELK = edu.doc_ti.jfcp.selec_reproc.storm.utils.Constants.ERROR_VALUE;
		}
		
		return valueToELK;
	}
	
	
	public void manageFieldsPostELK(Map<String, Object> cdrData) {
		
		String tripleta = ((cdrData.get(Constants.LTE_POTENCIALES) != null) ? (String) cdrData.get(Constants.LTE_POTENCIALES) : null);
		if (tripleta != null && !Constants.LTE_POTENCIALES_NOTVALUE.equalsIgnoreCase(tripleta)){
			cdrData.put(Constants.LTE_POTENCIALES, Constants.LTE_POTENCIALES_VALUE);
		}
		
		tripleta = ((cdrData.get(Constants.LTE_POTENCIALES_DST) != null) ? (String) cdrData.get(Constants.LTE_POTENCIALES_DST) : null);
		if (tripleta != null && !Constants.LTE_POTENCIALES_NOTVALUE.equalsIgnoreCase(tripleta)){
			cdrData.put(Constants.LTE_POTENCIALES_DST, Constants.LTE_POTENCIALES_VALUE);
		}
		
		if (cdrData.get(Constants.NODE) != null){
			managePostNode(cdrData);
		}
		
		Object objRatType = cdrData.get(Constants.RAT_TYPE);		
		if (objRatType != null){
			managePostRatType(cdrData, (String) objRatType);
		}

		if (cdrData.get("cell_ine_code") != null){
			managePostCellIneCode(cdrData, "cell_ine_code");
		}
		
		if (cdrData.get("cell_end_ine_code") != null) {
			managePostCellIneCode(cdrData, "cell_end_ine_code");
		}
		
		if (cdrData.get("cell_ini_ine_code") != null) {
			managePostCellIneCode(cdrData, "cell_ini_ine_code");
		}
		
		if (cdrData.get("cell_ini_band") != null) {
			managePostCellBand(cdrData, "cell_ini_band", "cell_ini");
		}
		
		if (cdrData.get("cell_end_band") != null) {
			managePostCellBand(cdrData, "cell_end_band", "cell_end");
		}
		
		if (cdrData.get("cell_band") != null) {
			managePostCellBand(cdrData, "cell_band", "cell");
		}
		
		if (cdrData.get(Constants.DATAFLOW) != null && 
				!((String)cdrData.get(Constants.CELL)).startsWith(Constants.IMSIBASEHLR)){
			manageRoaming(cdrData);			
		}
		
		if (cdrData.get(Constants.TARIFF_NAT_TRAFFIC) != null || cdrData.get(Constants.TARIFF_ROAM_TRAFFIC) != null) {
			manageTraffic(cdrData);
		}
		
	}


	private void managePostCellBand(Map<String, Object> cdrData, String field, String origin) {
		Object objCellBand = cdrData.get(field);
		Object objCell = cdrData.get(origin);
		if (objCellBand != null){
			String data = (String)objCellBand;
			String dataCell = (String)objCell;
			if(data != null && (data.equals(dataCell))){				
				cdrData.put(field, Constants.UNKNOWN);
			}
		}
	}




	private void manageTraffic(Map<String, Object> cdrData) {
		if (cdrData.get(Constants.TARIFF_NAT_TRAFFIC) != null) {
				if (((String)cdrData.get(Constants.TARIFF_NAT_TRAFFIC)).equals(Constants.TARIFF_DOUBLE_VALUE)) {
						cdrData.put(Constants.TARIFF_NAT_TRAFFIC, Constants.TARIFF_DOUBLE_VALUE_STR);
				} else {
					try {
						long lDato = Long.parseLong((String)cdrData.get(Constants.TARIFF_NAT_TRAFFIC));
						cdrData.put(Constants.TARIFF_NAT_TRAFFIC,lDato);
					} catch ( NumberFormatException e) {
						try {
							double lDato = Double.parseDouble((String)cdrData.get(Constants.TARIFF_NAT_TRAFFIC));
							cdrData.put(Constants.TARIFF_NAT_TRAFFIC,lDato);
						} catch ( NumberFormatException e2) {}
					}
				}
		}
		if (cdrData.get(Constants.TARIFF_ROAM_TRAFFIC) != null) {
			if (((String)cdrData.get(Constants.TARIFF_ROAM_TRAFFIC)).equals(Constants.TARIFF_DOUBLE_VALUE)) {
				cdrData.put(Constants.TARIFF_ROAM_TRAFFIC, Constants.TARIFF_DOUBLE_VALUE_STR);
			} else {
				try {
					long lDato = Long.parseLong((String)cdrData.get(Constants.TARIFF_ROAM_TRAFFIC));
					cdrData.put(Constants.TARIFF_ROAM_TRAFFIC,lDato);
				} catch ( NumberFormatException e) {
					try {
						double lDato = Double.parseDouble((String)cdrData.get(Constants.TARIFF_ROAM_TRAFFIC));
						cdrData.put(Constants.TARIFF_ROAM_TRAFFIC,lDato);
					} catch ( NumberFormatException e2) {}
				}
			}
		}
	}

	
	private void manageRoaming(Map<String, Object> cdrData) {
		if (cdrData.get(Constants.ROAMING_ZONE) != null) {
			cdrData.put(Constants.CELL_ZONE, (String)cdrData.get(Constants.ROAMING_ZONE));
			cdrData.put(Constants.CELL_NAME, Constants.ROAMING);
			cdrData.put(Constants.CELL_PROV, Constants.ROAMING);
		}
		if (cdrData.get(Constants.ROAMING_IMSI_OPERATOR) != null) {
			cdrData.put(Constants.CELL_MUNICIPIO, (String)cdrData.get(Constants.ROAMING_IMSI_OPERATOR));
		}
		if (cdrData.get(Constants.ROAMING_IMSI_PAIS) != null) {
			cdrData.put(Constants.CELL_SITE, (String)cdrData.get(Constants.ROAMING_IMSI_PAIS));
		}
	}

	
	private void managePostCellIneCode(Map<String, Object> cdrData, String field) {
		
		Object objCell = cdrData.get(field);
		if (objCell != null){
			String data = (String)objCell;
			if(data != null && data.equals("NULL")){
				data = "0";
				cdrData.put(field, data);
			}
		}
	}


	private int managePostRatType(Map<String, Object> cdrData, String ratType) {
		int ratValueOriginal = 0;
		try {
			ratValueOriginal = Integer.valueOf(ratType);
			if (ratValueOriginal == Constants.RAT_TYPE_VALUE){
				cdrData.put(Constants.CELL_TECNO, Constants.RAT_TYPE_CELL_TECNO);
			}
		} catch (NumberFormatException nfe){}
		return ratValueOriginal;
	}


	private String managePostNode(Map<String, Object> cdrData) {
		String valueToModify = null;
		String typeCdr = (String) cdrData.get(Constants.DATAFLOW);
		if (Constants.NODE_S.equalsIgnoreCase(typeCdr)) {
			if (cdrData.get(Constants.NODE_ACCESS) != null){
				valueToModify = (String) cdrData.get(Constants.NODE_ACCESS);
			}
		} else if (Constants.NODE_G.equalsIgnoreCase(typeCdr)){
			if (cdrData.get(Constants.NODE_SESSION) != null){
				valueToModify = (String) cdrData.get(Constants.NODE_SESSION);
			} 
		}
		
		if (valueToModify != null) {
			cdrData.put(Constants.NODE, valueToModify);
		}
		
		return valueToModify;
	}
	
	
	
	
	/**
	 * Process special fields to Files to HIVE
	 * @param cdrData
	 * @param calculatedField
	 * @param dataToProcess
	 * @return
	 * @throws Exception
	 */
	private static final List<String> CASE1_FIELDS_VOZ= Arrays.asList(
			Constants.CELL_END_BAND, Constants.CELL_END_CP, Constants.CELL_END_INE_CODE, 
			Constants.CELL_END_SITE, Constants.CELL_END_TECNO, Constants.CELL_END_ZONE,
			Constants.CELL_FIN_MUNICIPIO,  Constants.CELL_FIN_PROV,  Constants.CELL_FIN_RANGE
			);
	private static final List<String> CASE2_FIELDS_VOZ= Arrays.asList(
			Constants.CELL_INI_BAND, Constants.CELL_INI_CP, Constants.CELL_INI_INE_CODE, 
			Constants.CELL_INI_SITE, Constants.CELL_INI_TECNO, Constants.CELL_INI_ZONE,
			Constants.CELL_INI_MUNICIPIO,  Constants.CELL_INI_PROV,  Constants.CELL_INI_RANGE
			);
	private static final List<String> CASE3_FIELDS_VOZ= Arrays.asList(
			Constants.IMEI_DEVICE_MANUFACTURER, Constants.IMEI_DEVICE_MODEL, Constants.IMEI_DEVICE_SO, 
			Constants.IMEI_DEVICE_TECNO, Constants.IMEI_DEVICE_TYPE
			);
	private static final List<String> CASE4_FIELDS_VOZ= Arrays.asList(
			Constants.IMSI_SIM_GROUP
			);
	private static final List<String> CASE5_FIELDS_VOZ= Arrays.asList(
			Constants.MSISDN_DST_SEGMENTO, Constants.MSISDN_DST_TARIFF, Constants.MSISDN_DST_TARIFF_ID, Constants.MSISDN_STATUS_DST
			);
	private static final List<String> CASE6_FIELDS_VOZ= Arrays.asList(
			Constants.MSISDN_SEGMENTO, Constants.MSISDN_TARIFF, Constants.MSISDN_TARIFF_ID, Constants.MSISDN_STATUS
			);
	private static final List<String> CASE7_FIELDS_VOZ = Arrays.asList(
			Constants.NRN_NETWORK
			);
	private Map<String, Object> manageSpecialFieldsVozToHive(Map<String, Object> cdrData) {
		Map<String, Object> cdrDataToHive = cdrData;
		// Case (1) Se busca cell_end en el fichero de enriquecimiento topologia.txt. Si aparece se toma el campo correspondiente del fichero. Si no se etiqueta como "Desconocido"
		for (String field: CASE1_FIELDS_VOZ) {	
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}	
		// Case (2) Se busca cell_ini en el fichero de enriquecimiento topologia.txt. Si aparece se toma el campo correspondiente del fichero. Si no se etiqueta como "Desconocido"
		for (String field: CASE2_FIELDS_VOZ) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (3) Se busca imei_tac en el fichero tac.csv. Si no aparece ó toma valor "" ó valor "OTHER" se pone "Desconocido". En otro caso se pone el valor.
		for (String field: CASE3_FIELDS_VOZ) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("OTHER")) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (4) Se buscarán los primeros dígitos del IMSI en el fichero hlr.txt. Si no se encuentran ó aparece como "NoAplica" se marcará como "No Disponible" y si no se pondrá su valor.
		for (String field: CASE4_FIELDS_VOZ) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("NoAplica")) {
				cdrDataToHive.put(field, Constants.NO_DISPONIBLE);
			}
		}
		// Case (5) Se busca msisdn_dst_homogeneo en el fichero de enriquecimiento tarifas.txt. Si no aparece ó toma valor "" ó valor "OTHER" ó valor "Not found" se pone "Desconocido". En otro caso se pone el valor.
		for (String field: CASE5_FIELDS_VOZ) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("OTHER") || cdrData.get(field).toString().equalsIgnoreCase("Not found")) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (6) Se busca msisdn_homogeneo en el fichero de enriquecimiento tarifas.txt.  Si no aparece ó toma valor "" ó valor "OTHER" ó valor "Not found" se pone "Desconocido". En otro caso se pone el valor.
		for (String field: CASE6_FIELDS_VOZ) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("OTHER") || cdrData.get(field).toString().equalsIgnoreCase("Not found")) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (7) Se buscará en el fichero de enriquecimiento nrn.txt. Si no aparece tomará el valor "Desconocido".
		for (String field: CASE7_FIELDS_VOZ) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		return cdrDataToHive;
	}
	private static final List<String> CASE1_FIELDS_DATOS = Arrays.asList(
			Constants.CELL_BAND, Constants.CELL_CP, Constants.CELL_INE_CODE, 
			Constants.CELL_SITE, Constants.CELL_TECNO, Constants.CELL_ZONE,
			Constants.CELL_MUNICIPIO,  Constants.CELL_PROV,  Constants.CELL_RANGE
			);
	private static final List<String> CASE2_FIELDS_DATOS = Arrays.asList(
			Constants.IMSI_SIM_GROUP
			);
	private static final List<String> CASE3_FIELDS_DATOS = Arrays.asList(
			Constants.NODE_SESSION
			);
	private static final List<String> CASE4_FIELDS_DATOS = Arrays.asList(
			Constants.CLOSING_CAUSE
			);
	private static final List<String> CASE5_FIELDS_DATOS = Arrays.asList(
			Constants.CLOSING_CONDITION
			);
	private static final List<String> CASE6_FIELDS_DATOS = Arrays.asList(
			Constants.IMEI_DEVICE_MANUFACTURER, Constants.IMEI_DEVICE_MODEL, Constants.IMEI_DEVICE_SO,
			Constants.IMEI_DEVICE_TECNO, Constants.IMEI_DEVICE_TYPE
			);
	private static final List<String> CASE7_FIELDS_DATOS = Arrays.asList(
			Constants.NODE_ACCESS
			);
	private static final List<String> CASE8_FIELDS_DATOS= Arrays.asList(
			Constants.MSISDN_CF_TRAFICO_NACIONAL, Constants.MSISDN_CF_TRAFICO_ROAMING, Constants.MSISDN_CICLO_FACTURACION, 
			Constants.MSISDN_FUP_NACIONAL, Constants.MSISDN_FUP_ROAMING,
			Constants.MSISDN_SEGMENTO, Constants.MSISDN_TARIFF, Constants.MSISDN_TARIFF_ID, Constants.MSISDN_STATUS
			);
	private static final List<String> CASE34_FIELDS_DATOS = Arrays.asList(
			Constants.GGSN_ADDR
			);

	private Map<String, Object> manageSpecialFieldsDatoToHive(Map<String, Object> cdrData) {
		Map<String, Object> cdrDataToHive = cdrData;
		// Case (1) Se busca cell en el fichero de enriquecimiento topologia.txt. Si aparece se toma el campo correspondiente del fichero. Si no se etiqueta como "Desconocido"
		for (String field: CASE1_FIELDS_DATOS) {	
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				LOG.info("Cambio valor a -Desconocido- si no existe: " + field);
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}	
		// Case (2) Se buscarán los primeros dígitos del IMSI en el fichero hlr.txt. Si no se encuentran ó aparece como "NoAplica" se marcará como "No Disponible" y si no se pondrá su valor.
		for (String field: CASE2_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("NoAplica")) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (3) Si aparece en el fichero de enriquecimiento ggsn.txt se pondrá su nombre. Si no se marcará como "Desconocido".
		for (String field: CASE3_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (4) Se busca el campo "Causa de cierre del CDR" en el fichero causeforrecclosing.txt. Si apoarece se pone su valor y si no se pone "Desconocido".
		for (String field: CASE4_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (5) Se busca el campo "Condición de cambio " en el fichero change_condition.txt.  Si aparece se pone su valor y si no se pone "Desconocido".
		for (String field: CASE5_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (6) Se busca imei_tac en el fichero tac.csv. Si no aparece ó toma valor "" ó valor "OTHER" se pone "Desconocido". En otro caso se pone el valor.
		for (String field: CASE6_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("OTHER")) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (7) Si aparece en el fichero de enriquecimiento sgsn.txt se pondrá su nombre. Si no se marcará como "Desconocido".
		for (String field: CASE7_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (8) Se busca msisdn_homogeneo en el fichero de enriquecimiento tarifas.txt.  Si no aparece ó toma valor "" ó valor "OTHER" ó valor "Not found" -o valor "-999" se pone "Desconocido". En otro caso se pone el valor.
		for (String field: CASE8_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty() || cdrData.get(field).toString().equalsIgnoreCase("OTHER")
				|| cdrData.get(field).toString().equalsIgnoreCase("Not Found") || cdrData.get(field).toString().equalsIgnoreCase("-999")) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		// Case (34) Si aparece en el fichero de enriquecimiento ggsn.txt se pondrá su nombre. Si no se marcará como "Desconocido".
		for (String field: CASE34_FIELDS_DATOS) {
			if (cdrData.get(field) == null || cdrData.get(field).toString().isEmpty()) {
				cdrDataToHive.put(field, Constants.UNKNOWN);
			}
		}
		return cdrDataToHive;
	}
	
	public Map<String, Object> manageSpecialFieldsToHive(String type, Map<String, Object> cdrData) {
		if (Constants.CDRS_FIELD_VOZ.equals(type)) {			
			return manageSpecialFieldsVozToHive(cdrData);

		} else if (Constants.CDRS_FIELD_DATO.equals(type)) {
			return manageSpecialFieldsDatoToHive(cdrData);
		}
		
		return cdrData;
	}
}
