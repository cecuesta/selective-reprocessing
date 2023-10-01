
MODE=$1

if [ "$MODE" == "CLUSTER" ]
then
   BROKERS=kafka01:9092,kafka02:9092,kafka03:9092
else
   BROKERS=kafka:9092
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR/java-gendata/

docker exec -it flume java -cp /tmp/file-generator-1.0.0-dep.jar edu.doc_ti.jfcp.selec_reproc.gendata.KafkaGenerator -t topic_in_stream -n 500 -b $BROKERS

