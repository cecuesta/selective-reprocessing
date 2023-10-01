


NUM_FILES=$1
SECONDS_BETWEEN_FILES=$2

if [ "$NUM_FILES" == "" ]
then
   NUM_FILES=8
fi

if [ "$SECONDS_BETWEEN_FILES" == "" ]
then
   SECONDS_BETWEEN_FILES=5
fi

echo Generate $NUM_FILES every $SECONDS_BETWEEN_FILES seconds
echo

docker exec -it flume java -cp /tmp/file-generator-1.0.0-dep.jar edu.doc_ti.jfcp.selec_reproc.gendata.FileGenerator -p /tmp/flume-input -s $SECONDS_BETWEEN_FILES -m $NUM_FILES

