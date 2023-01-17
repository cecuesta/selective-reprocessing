package edu.doc_ti.jfcp.selec_reproc.storm.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public final class Constants {
	    
	public final static String KAFKA_SPOUT_ID = "kafkaSpout";
	public final static String PROCESS_BOLT_ID = "processBolt";
	public final static String INSERT_BOLT_ID = "insertESBolt" ;
	public final static String KAFKA_INSERT_BOLT_ID = "kafkaInsertBolt";
	public final static String HDFS_BOLT_ID = "hdfsInsertBolt";
	public final static String KAFKA_OUT_DEFAULT_BOLT_ID = "aggregate";
	
	public final static String DELIMITER = ",";
	
	public final static String ERROR = "error_data";
	public final static String ERROR_VALUE = "invalid_data";
	public final static String NOPROCESS_VALUE = "no_process";
	
	public final static String INDEX_DEFAULT = "cdrs-*/cdr";
	public final static String INDEX_TOPIC_ERR = "error-";
	
	public static final String KAFKA_BOLT_KEY = "key";
	
	public final static String DATEFORMAT_TO_INDEXELK = "yyyy.MM.dd";
	public final static String DATEFORMAT_TO_KAFKA = "yyyy_MM_dd'T'HH";
	public final static String DATEFORMAT_DATEELK = "yyyy-MM-dd'T'HH:mm:ssZ";
	public final static String DATEFORMAT_VOICEFIELDS = "yyyyMMddHHmmss";
	public final static String DATEFORMAT_DATAFIELDS = "yyMMddHHmmssZ";
	public final static String DATEFORMAT_KAFKADATES = DATEFORMAT_VOICEFIELDS;
	public final static String DATEFORMAT_DAY = "yyyyMMdd";
	public final static String DATEFORMAT_HOUR = "HHmmss";
	
	public final static String PROCESSINGDATE = "_proc_date";
	public final static String BOLTID = "_boltid";
	public final static String RAWDATA = "rawdata";
	public final static String FILENAME = "filename";
	public final static String POSITION = "line_number";
	public final static String TIMESTAMP = "@timestamp";
	public final static String INITDATE = "date_init";
	public final static String INITDATE_DAY = "date_init_dia";
	public final static String INITDATE_HOUR 				= "date_init_hora";
	public final static String INITDATE_DAYHOUR 			= "date_init_diahora";
	public final static String ENDDATE 						= "date_end";	
	public final static String ENDDATE_DAY 					= "date_end_dia";
	public final static String ENDDATE_HOUR 				= "date_end_hora";
	public final static String ENDDATE_DAYHOUR 				= "date_end_diahora";
	public final static String DAY 							= "date_dia";
	public final static String HOUR 						= "date_hora";
	public final static String DAYHOUR 						= "date_diahora";
	public final static String LTE_POTENCIALES 				= "triplet";
	public final static String LTE_POTENCIALES_DST 			= "triplet_dst";
	public final static String LTE_POTENCIALES_NOTVALUE 	= "NO";
	public final static String LTE_POTENCIALES_VALUE 		= "SI";
	public final static String HLRI 						= "hlrI";
	public final static String HLRF 						= "hlrF";
	public final static String MCCMNC 						= "mcc_mnc";
	public final static String IMEITAC 						= "imei_tac";
	public final static String NPXMSISDN 					= "msisdn_";
	public final static String POS_FALLO 					= "fault_position";
	public final static String CAUSA_INTERNA 				= "fault_internal_cause";
	public final static String MSCNAME 						= "msc_name";
	public final static String ROUTEIN 						= "route_in";
	public final static String ROUTEOUT 					= "route_out";
	public final static String ROUTEIN_I 					= "I";
	public final static String ROUTEOUT_O 					= "O";
	public final static String TRANSACTION 					= "transaction";
	public final static String TRANSACTION_I 				= ROUTEIN_I;
	public final static String TRANSACTION_O 				= ROUTEOUT_O;
	public final static String CALLFLOW 					= "call_flow";
	public final static String DATAFLOW 					= "data_flow";
	public final static String NODE 						= "node";
	public final static String NODE_ACCESS 					= "node_access";
	public final static String NODE_SESSION 				= "node_session";
	public final static String NODE_S 						= "sgsn";
	public final static String NODE_G 						= "ggsn";
	public final static String GGSN_ADDR 					= "ggsn_addr";
	public final static String CELL 						= "cell";
	public final static String CELL_TECNO 					= "cell_tecno";
	public final static String CELL_ZONE 					= "cell_zone";
	public final static String CELL_NAME 					= "cell_name";
	public final static String CELL_PROV 					= "cell_prov";
	public final static String CELL_MUNICIPIO 				= "cell_municipio";
	public final static String CELL_SITE 					= "cell_site";
	public final static String ROAMING_ZONE 				= "roaming_zone";
	public final static String ROAMING_IMSI_OPERATOR 		= "roaming_imsi_operador";
	public final static String ROAMING_IMSI_PAIS 			= "roaming_imsi_pais";
	public final static String ROAMING 						= "ROAMING";
	public final static String IMSI_OPERATOR 				= "imsi_operator";
	public final static String IMSI_COUNTRY 				= "imsi_country";
	public final static String LOC_AREACODE 				= "loc_areaCode";
	public final static String RAT_TYPE 					= "rat_type_cell";
	public final static String RAT_TYPE_CELL_TECNO 			= "VoWIFI";
	
	public final static String NSA_UPLOAD_VOLUME 			= "upload_volume_5g";
	public final static String NSA_DOWNLOAD_VOLUME 			= "download_volume_5g";
	public final static String NSA_START_TIME 				= "start_time_5g";
	public final static String NSA_DURATION 				= "duration_5g";
	public final static String NSA_RAT_TYPE 				= "rat_type_5g";
	
	public final static String CELL_BAND 					= "cell_band";
	public final static String CELL_CP 						= "cell_CP";
	public final static String CELL_INE_CODE				= "cell_ine_code";
	/*
	public final static String CELL_SITE 					= "cell_site";
	public final static String CELL_TECNO 					= "cell_tecno";
	public final static String CELL_ZONE 					= "cell_zone";
	public final static String CELL_MUNICIPIO				= "cell_municipio";
	public final static String CELL_PROV					= "cell_prov";
	*/
	public final static String CELL_RANGE					= "cell_range";
	
	public final static String CELL_END		 				= "cell_end";
	public final static String CELL_END_BAND 				= "cell_end_band";
	public final static String CELL_END_CP 					= "cell_end_CP";
	public final static String CELL_END_INE_CODE			= "cell_end_ine_code";
	public final static String CELL_END_SITE 				= "cell_end_site";
	public final static String CELL_END_TECNO 				= "cell_end_tecno";
	public final static String CELL_END_ZONE 				= "cell_end_zone";
	public final static String CELL_FIN_MUNICIPIO			= "cell_fin_municipio";
	public final static String CELL_FIN_PROV				= "cell_fin_prov";
	public final static String CELL_FIN_RANGE				= "cell_fin_range";
	
	public final static String CELL_INI		 				= "cell_ini";
	public final static String CELL_INI_BAND 				= "cell_ini_band";
	public final static String CELL_INI_CP 					= "cell_ini_CP";
	public final static String CELL_INI_INE_CODE			= "cell_ini_ine_code";
	public final static String CELL_INI_SITE 				= "cell_ini_site";
	public final static String CELL_INI_TECNO 				= "cell_ini_tecno";
	public final static String CELL_INI_ZONE 				= "cell_ini_zone";
	public final static String CELL_INI_MUNICIPIO			= "cell_ini_municipio";
	public final static String CELL_INI_PROV				= "cell_ini_prov";
	public final static String CELL_INI_RANGE				= "cell_ini_range";
	
	public final static String IMEI_DEVICE_MANUFACTURER		= "imei_device_manufacturer";
	public final static String IMEI_DEVICE_MODEL			= "imei_device_model";
	public final static String IMEI_DEVICE_SO				= "imei_device_so";
	public final static String IMEI_DEVICE_TECNO			= "imei_device_tecno";
	public final static String IMEI_DEVICE_TYPE				= "imei_device_type";
	
	public final static String QCI 							= "qci";
	
	public final static String CELL_ANCHOR                  = "cell_anchor";
	public final static String CELL_INI_ANCHOR              = "cell_ini_anchor";
	public final static String CELL_END_ANCHOR              = "cell_end_anchor";
	
	public final static String IMSI_SIM_GROUP				= "imsi_sim_group";
	public final static String MSISDN_DST_SEGMENTO			= "msisdn_dst_segmento";
	public final static String MSISDN_DST_TARIFF			= "msisdn_dst_tariff";
	public final static String MSISDN_DST_TARIFF_ID			= "msisdn_dst_tariff_id";
	public final static String MSISDN_SEGMENTO				= "msisdn_segmento";
	public final static String MSISDN_STATUS				= "msisdn_status";
	public final static String MSISDN_STATUS_DST			= "msisdn_status_dst";
	public final static String MSISDN_TARIFF				= "msisdn_tariff";
	public final static String MSISDN_TARIFF_ID				= "msisdn_tariff_id";
	public final static String MSISDN_CF_TRAFICO_NACIONAL	= "msisdn_cf_trafico_nacional";
	public final static String MSISDN_CF_TRAFICO_ROAMING	= "msisdn_cf_trafico_roaming";
	public final static String MSISDN_CICLO_FACTURACION 	= "msisdn_ciclo_facturacion"; 
	public final static String MSISDN_FUP_NACIONAL			= "msisdn_fup_nacional";
	public final static String MSISDN_FUP_ROAMING			= "msisdn_fup_roaming";
	
	public final static String NRN_NETWORK					= "nrn_network";
	
	public final static String CLOSING_CAUSE 				= "closing_cause";
	public final static String CLOSING_CONDITION			= "closing_condition";		
	
	public final static String UNKNOWN = "Desconocido";
	public final static String NO_DISPONIBLE = "No Disponible";
	
	public final static int RAT_TYPE_VALUE = 3;
	
	public final static String NODATA = "Not registered as a valid CDR";
	
	public final static String IMSIBASEHLR = "21403";
	public final static String IMSIBASEHLR_SPECIAL= "21401";
	
	public final static String IMSIBASEHLR_SPECIAL_FLOW= "ggsn";
	
	public final static int MCCMNC_LENGTH = 5;
	
	public final static int IMEITAC_LENGTH = 8;
	
	public final static String TARIFF_NAT_TRAFFIC = "msisdn_cf_trafico_nacional";
	public final static String TARIFF_ROAM_TRAFFIC = "msisdn_cf_trafico_roaming";
	public final static String TARIFF_DOUBLE_VALUE = "-999";
	public final static String TARIFF_DOUBLE_VALUE_STR = "Desconocido";
	
	public final static int MSISDN = 9;
	public final static int MSISDN_34 = 11;
	public final static int MSISDN_0034 = 13;
	public final static int MSISDN_3459 = 15;
	public final static int MSISDN_003459 = 17;
	
	public final static String MSISDN_34_VAL = "34";
	public final static String MSISDN_0034_VAL = "0034";
	public final static String MSISDN_3459_VAL = "3459";
	public final static String MSISDN_003459_VAL = "003459";

	public final static int INPUT_POSITION_DATA_VAL = 2;
	
	public final static String CDRS_LOOKUPS_NAME = "lcdr";	
	public final static int CDRS_LOOKUPS_NUMBER_DEFAULT = 25;
	
	public final static String CDRS_TYPE_T = "t";
	public final static String CDRS_TYPE_ENDKEY = ".key.position";
	public final static String CDRS_TYPE_ENDPOS = ".positions";
	public final static String CDRS_TYPE_ENDTYPE = ".type";
	public final static String CDRS_TYPE_ENDFLOW = ".flow";
	
//	public final static String CDRS_FIELD_INITBASENAME = "cdr.field";
	public final static String CDRS_FIELD_INITBASENAME = "cdr.";
	public final static String CDRS_FIELD_DATO = "dato";
	public final static String CDRS_FIELD_VOZ = "voz";
	public final static String CDRS_FIELD_ENDNAME = ".name";
	public final static String CDRS_FIELD_ENDPOS = ".position";
	public final static String CDRS_FIELD_CALCULATED = ".calculated";
	public final static String CDRS_FIELD_TYPE = ".type";
	public final static int CDRS_FIELDS_NUMBER_DEFAULT = 47;
	
	public final static int CDRS_TYPES_NUMBER_DEFAULT = 11;
	
	public final static String K_ELK_HOST_DAFAULT = "10.200.2.150,10.200.2.151,10.200.2.152";
	
	public final static int INDEX_POSITION = 0;
	public final static int DOCTYPE_POSITION = 1;
	public final static int FILENAME_POSITION = 2;
	public final static int OUT_TOPIC_POSITION = 3;
	public final static int DATA_POSITION = 4;
	
	public static final Map<Integer, String> MAP_FIELDS_REDIRECT = ImmutableMap.of(
			INDEX_POSITION, "index",
			DOCTYPE_POSITION, "type",
			FILENAME_POSITION, "filename",
			OUT_TOPIC_POSITION, "outTopic",
			DATA_POSITION, "data"
	);
	
	public static final List<String> MAP_TYPEFIELDS = Arrays.asList("long", "string", "integer", "date");
	
	public final static String RELOAD_FILE_PRFX = "reload:";

	
	public static final Object SYSTEM_COMPONENT_ID = "__system";
	public static final Object SYSTEM_TICK_STREAM_ID =  "__tick" ;
	
	
	public static final String K_TOPIC_NAME = "kafka.topicname";
	public static final String K_STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root" ;
	public static final String K_KAFKA_ZOOKEEPER = "kafka.zookeeper";
	public static final String K_KAFKA_BOOTSTRAPSERVERS = "kafka.bootstrap.servers";
	public static final String K_STORM_NIMBUS_HOST = "storm.nimbus.host" ;
	
	public static final String K_NUMWORKERS = "numworkers" ;
	public static final String K_NUM_KAFKA_SPOUTS = "num.kafka.spouts";
	public static final String K_NUM_ES_BOLTS = "num.es_insert.bolts";

	public static final String K_STORM_CONFIG_FILE = "config.file";
	public static final Object K_ELK_INDEX_NAME = "elk.indexname";

	
    public final static String K_ELK_BATCH_SIZE 			= "elk.batch.size" ;
    public final static String K_ELK_BATCH_TAM_MB 			= "elk.batch.tam.mb" ;
    public final static String K_ELK_BATCH_FLUSH_SECS		= "elk.batch.flush.seconds" ;
    public final static String K_ELK_BATCH_CONCURRENT 		= "elk.batch.concurrence" ;
	public final static String K_ELK_HOST 					= "elk.bulk.hosts";
    public final static String K_ELK_USER				  	= "elk.user" ;
    public final static String K_ELK_PASSWD				  	= "elk.passwd" ;	
}
