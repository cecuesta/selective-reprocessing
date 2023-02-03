
# Selective reprocessing mechanism testing enviroment

## Purpose

 This is the software to configure a testing enviroment for the selective reprocessing mechanism
 The enviroment is built with dockers, and there are 2 possible options: 
 - Tools configured in a non-cluster way
 - Tools configured as a cluster

 This docker enviroment is quite complex and it is only configured for testing pourposes
 More exhaustive tests have been done in a full on-premise cluster

## Requisites
 To execute the test following tools must be installed:
 - docker
 - jdk 1.8 or greater
 - maven 3.0.5 or greater
 - git version 1.8.3.1 or greater

 this enviroment has been tested in Centos 7,8,9 and Ubuntu 20.4, 22.4

## Configuration
 first step should be clone github repository with following command:
    git clone https://github.com/doc-ti/selective-reprocessing.git

 - to configure a non-cluster enviroment execute: **configure-basic.sh**
 - to configure a clustered enviroment execute: **configure-cluster.sh**

## Java projects building
 3 java projects are built locally, many dependencies had to be downloaded

 - storm code to process data
 - flume pluging to process files and to insert into mysql
 - random file generator to simulate the process

## Docker creation
 Several docker containers are created, so different images must be downloaded

 - Zookeeper (needed for Storm and Kafka)
 - Kafka (3 in cluster environment)
 - Apache Storm Nimbus
 - Apache Storm UI
 - Apache Storm Supervisor (3 in cluster environment)
 - Mysql
 - Flume
 - Elasticsearch (3 in cluster environment)

## Execution of tests
 Data files are generated with the following command executed in the host enviroment (execute with -h option for more information):
 
 > docker exec -it flume java -cp /tmp/file-generator-1.0.0-dep.jar edu.doc_ti.jfcp.selec_reproc.gendata.FileGenerator -p /tmp/flume-input -s 5 -m 6

 File are generated into flume container
 Flume process these files, inserting data into Kafka and metadata info into Mysql
 Storm process kafka data and insert into Elasticsearch
 Finally it is possible to check that all the data is properly inserted into Mysql checking the number of records from the file against the real number inserted into Elasticseach

This can be done using the script : check-files.sh

## Warnings in docker creation & configuration

 Single configuration for dockers is more likely to work properly as it is more simple
 Elasticsearch in cluster need some configuration at OS level, so docker startup could fail
 if OS is not properly configured for elasticsearch
 Additional information can be found on : https://www.elastic.co/guide/en/elasticsearch/reference/7.9/bootstrap-checks.html

## Needed tools instalation

 In a clean enviroment these are the commands needed to install the tools
 and something extra for some environments


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

systemctl start docker

