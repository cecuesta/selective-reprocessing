package edu.doc_ti.jfcp.selec_reproc.storm.utils;

public class Keys {

	public static final String K_NUM_PROCESS = "num.process.bolts";
	public static final String K_NUM_HDFS = "num.hdfs.bolts";
	public static final String K_NUM_KAFKA_BOLTS = "num.kafka_insert.bolts";
	
	public static final String WORKER_MAXHEAPSIZE = "topology.worker.max.heap.size.gb";
	public static final String WORKER_CHILDOPTS = "topology.worker.childopts";

	public final static String CDRS_TYPES = "cdr.types.process";
	public final static String CDRS_FORCE_NUMBER_PROCESS = "cdr.force.number.process";
	
	public final static String CDRS_FIELDS_DATO_NUMBER = "cdr.dato.fields.number";
	public final static String CDRS_FIELDS_VOZ_NUMBER = "cdr.voz.fields.number";
	
	public final static String CDRS_FIELDS_VOZ_NOTPRINT = "cdr.voz.not.print";
	public final static String CDRS_FIELDS_DATO_NOTPRINT = "cdr.dato.not.print";
	
	public final static String K_OUT_BASE_TOPIC_NAME = "kafka.output.topicname";
	public final static String K_OUT_BASE_TOPIC_ENABLED = "kafka.output.enabled";
	

	public final static String K_OUT_ELK_INSERT_ENABLED = "elk.insert.enabled";

	
	public final static String K_INIT_DATE = "kafka.process.from.initdate";
	public final static String K_END_DATE = "kafka.process.to.enddate";
	
//	public static final String HDFS_BOLT_DELIMITER = "hdfs.field.delimiter";
//	public static final String HDFS_SIZE_FILE = "hdfs.file.rotation.size.mb";
	public static final String HDFS_BATCH = "hdfs.batch.size";
	public static final String HDFS_FOLDER = "hdfs.path";
	public static final String HDFS_CLUSTER = "hdfs.cluster";
	public static final String HDFS_PORT = "hdfs.port";
	public static final String HDFS_ENABLED = "hdfs.enabled";
	
	public static final String WRITE_FILES = "write.files.enabled";
	public static final String WRITE_FILES_TIMEOUT = "write.files.timeout";
	public static final String WRITE_FILES_MAX_RECORDS = "write.files.max_records";
	public static final String WRITE_FILES_OUTPATH_TMP = "write.files.outpath.tmp";
	public static final String WRITE_FILES_OUTPATH_FINAL = "write.files.outpath.final";
	public static final String WRITE_FILES_HEADERS = "write.files.headers";
	

}
