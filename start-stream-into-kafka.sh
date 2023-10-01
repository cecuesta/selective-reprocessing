

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR/java-gendata/

docker exec -it flume java -cp /tmp/file-generator-1.0.0-dep.jar edu.doc_ti.jfcp.selec_reproc.gendata.KafkaGenerator -t topic_in_stream -b kafka:9092 -n 500

