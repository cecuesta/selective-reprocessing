

# This is the software to configure a testing enviroment for the selective reprocessing mechanism
# The enviroment is built with dockers, and there are 2 possible options: 
#    - Tools configured in a non-cluster way
#    - Tools configured as a cluster
#
# This docker enviroment is quite complex and it is only configured for testing pourposes
# More exhaustive tests have been done in a full on-premise cluster
#
# To execute the test followin tools must be installed:
#    - docker
#    - jdk 1.8 or greater
#    - maven 3.0.5 or greater
#    - git version 1.8.3.1 or greater
#
# this enviroment has been tested in Centos 7 and ??
#
# first step should be clone github repository with following command:
#    git clone https://github.com/doc-ti/selective-reprocessing.git
#
# to configure a non-cluster enviroment execute configure-basic.sh
#
# to configure a clustered enviroment execute configure-cluster.sh
#
