


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


cd $SCRIPT_DIR


rm docker-compose.yml

if [ "$?" -ne 0 ] ; then
   echo Exiting .... 
   exit
else
   echo Link docker-compose.yml removed
fi


ln docker-compose-basic.yml docker-compose.yml
if [ "$?" -ne 0 ] ; then
   echo Exiting .... 
   exit
else
   echo Link docker-compose.yml recreated
fi

export SR_DIR=${SCRIPT_DIR}

cd ${SR_DIR}/java-flume
mvn clean install -Dmaven.wagon.http.ssl.insecure=true

if [ "$?" -ne 0 ] ; then
   echo Exiting ....
   exit
else
   echo flume code created
fi

cd ${SR_DIR}/java-storm
mvn clean install assembly:single -Dmaven.wagon.http.ssl.insecure=true

if [ "$?" -ne 0 ] ; then
   echo Exiting ....
   exit
else
   echo storm code created
fi

cd ${SR_DIR}/java-gendata
mvn clean install assembly:single -Dmaven.wagon.http.ssl.insecure=true

if [ "$?" -ne 0 ] ; then
   echo Exiting ....
   exit
else
   echo storm code created
fi

cd ${SR_DIR}
ln -s ../java-flume/target/plugin-flume-1.0.0.jar flume-conf/
ln -s ../java-gendata/target/file-generator-1.0.0-dep.jar flume-conf/
ln -s ../java-storm/target/sr-storm-1.0.0-dep.jar storm-conf/

cd ${SR_DIR}/flume-conf
ln -s flume-single.conf flume.conf

cd ${SR_DIR}
docker-compose up -d

if [ "$?" -ne 0 ] ; then
   echo Exiting ....
   exit
else
   echo docker environment complete
fi

docker stop flume
docker start flume

docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic_data --replication-factor 1 --partitions 6


