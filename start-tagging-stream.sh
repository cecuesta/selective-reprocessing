

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR/java-gendata/

docker exec -it flume java -cp /tmp/kafka-streams-sr-0.1-dep.jar edu.doc_ti.jfcp.selec_reproc.kafkastreams.TaggingTopology -b kafka:9092 -i topic_in_stream -o topic_data -u "jdbc:mysql://mysql/mydatabase?user=myuser&password=rootpass"

