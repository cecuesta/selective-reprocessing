
# Selective reprocessing mechanism testing enviroment

## Purpose of this kit

 This is the software to fully configure a testing enviroment for the selective reprocessing mechanism
 The enviroment is built with dockers, and there are 2 possible options: 
 - Tools configured in a non-cluster way
 - Tools configured as a cluster

 This docker enviroment is quite complex and it is only configured for testing pourposes. 
 More exhaustive tests have been performed in a full on-premise cluster

## What to reproduce

With this kit it is possible to create a docker environment with the necesary tools for a real-time big data processing system.

Aditionally to the docker environment several java projects must be compiled.

Once the environment is configured and running, it can be tested.

Test cover two different scenarios:

1.- Generation of random files that are inserted in Kafka using flume, processed with Storm and loaded in Elasticsearch.
The information of each file is stored in a mysql database as they are inserted in Kafka. Once loaded there is a checking process to verify that the same data from the original file is loaded into Elasticsearch.

2.- Generation of data directly inserted into kafka. This data stream is processed with Storm and loaded in Elasticsearch.
The information of the input stream is tagged and stored again in kafka using a kafka-streams topology. Tagging groups are stored in a mysql database. Once loaded there is a checking process to verify that the same data from the original kafka stream is loaded into Elasticsearch.

If the system is very stressed (high volume of input data, Elasticsearch queries or other situations) there should be load failures that would be detected by this mechanism.


## Requisites
 To execute the test following tools must be installed:
 - docker
 - jdk 1.8 or greater
 - maven 3.0.5 or greater
 - git version 1.8.3.1 or greater

 this enviroment has been tested in Centos 7,8,9 and Ubuntu 20.4, 22.4
 
Use of a virtual machine is recommended. We have used a VM with 8 cores, 16 GB RAM, 160 GB HDD for testing

## Configuration
 first step should be clone github repository with following command:
    git clone https://github.com/doc-ti/selective-reprocessing.git

 - to configure a non-cluster enviroment execute: ***configure-basic.sh***
 - to configure a clustered enviroment execute: ***configure-cluster.sh***

## Java projects building
 4 java projects are built locally, many dependencies had to be downloaded. 
 This java built is included in the installation process.

 - storm code to process data
 - flume pluging to process files and to insert into mysql
 - random file generator to simulate the process
 - kafka streams topology to tag a input topology

## Docker creation
 Several docker containers are created, so different images must be downloaded

 - Zookeeper (needed for Storm and Kafka)
 - Kafka (3 in cluster environment)
 - Apache Storm Nimbus
 - Apache Storm UI
 - Apache Storm Supervisor (2 in cluster environment)
 - Mysql
 - Flume
 - Elasticsearch (3 in cluster environment)

## Execution of tests

### Input from file
 Data files are generated with the following command executed in the host enviroment (you can execute then java class directly with -h option for more information):
 
 ***generate-files.sh [num files to generate] [seconds between files]***

 File are generated into flume container
 Flume process these files, inserting data into Kafka and tagging metadata info into mysql
 Storm process kafka tagged data and insert into Elasticsearch

### Input directly in a kafka topic
 Data is inserted directly into a kafka topic using a data fake generator, generate process is started using: ***start-stream-into-kafka.sh*** (use the parameter CLUSTER to load into the cluster configuration)

 This first stream is readed from the initial topic and tagged using a kafka-streams topology. This topology can be started using: ***start-tagging-stream.sh*** (use the parameter CLUSTER to load into the cluster configuration)

 Storm process kafka tagged data and insert into Elasticsearch

### Checking tags (both from stream and from files)
 Finally it is possible to check that all the data is properly inserted into mysql checking the number of records from the file against the real number inserted into Elasticseach

This can be done using the script : ***check-files.sh***

In both scenarios it is possible to manually generate random errors in the check process, this can be done deleting random records (script ***delete-random-records.sh***) or inserting random records (script ***insert-random-records.sh***) 


## Warnings in docker creation & configuration

 Single configuration for dockers is more likely to work properly as it is more simple
 Elasticsearch in cluster need some configuration at OS level, so docker startup could fail
 if OS is not properly configured for elasticsearch
 Additional information can be found on : https://www.elastic.co/guide/en/elasticsearch/reference/7.9/bootstrap-checks.html

## Needed tools instalation

 In a clean enviroment these are the commands needed to install the tools
 and some extra commands are also provided


### CentOS / Red Hat

yum -y install git java maven yum-utils

yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

yum -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin

ln -s /usr/libexec/docker/cli-plugins/docker-compose /usr/local/bin/

sysctl -w vm.max_map_count=262144 # configuration needed for Elasticsearch

systemctl start docker

   
### Ubuntu

apt-get -y update

apt-get -y install git openjdk-8-jdk-headless maven docker*

sysctl -w vm.max_map_count=262144 # configuration needed for Elasticsearch

systemctl start docker

