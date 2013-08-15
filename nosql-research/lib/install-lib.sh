#!/bin/sh

mvn install:install-file -Dfile=aerospike-client-3.0.6.jar -DgroupId=com.aerospike -DartifactId=aerospike-client -Dversion=3.0.6 -Dpackaging=jar
