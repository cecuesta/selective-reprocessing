


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


cd $SCRIPT_DIR

if [ -e docker-compose.yml ] ; then
  rm docker-compose.yml
fi

ln -s docker-compose-cluster.yml docker-compose.yml
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

if [ -e flume.conf ] ; then rm flume.conf ; fi
ln -s flume-cluster.conf flume.conf

cd ${SR_DIR}
docker-compose up -d

if [ "$?" -ne 0 ] ; then
   echo Exiting ....
   exit
else
   echo docker environment complete
fi


echo "Waiting 20 seconds to mysql to boot"
for NN in $(seq 1 20)
do
  echo $NN ...
  sleep 1
done

for CRED in "" "-u root -prootpass "
do
  docker exec -it mysql mysql $CRED -e "CREATE database mydatabase ; "
  docker exec -it mysql mysql $CRED -e "CREATE USER 'myuser'@'%' IDENTIFIED BY 'rootpass';"
  docker exec -it mysql mysql $CRED -e "GRANT ALL PRIVILEGES ON *.* TO 'myuser'@'%' WITH GRANT OPTION;"  
  docker exec -it mysql mysql $CRED -e "FLUSH PRIVILEGES ;"
done

docker stop flume
docker start flume


docker exec -it mysql mysql -u myuser -prootpass -e "show databases;" 

docker exec -it kafka01 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic_data --replication-factor 3 --partitions 12

docker exec -it nimbus storm jar /tmp/sr-storm-1.0.0-dep.jar LoadTopology /tmp/topology-cluster.properties topology-load-data


