#!/bin/bash
MYNAME=${BASH_SOURCE-$0}
BIN_DIR=$(dirname $MYNAME)
BASE_DIR=$(cd ${BIN_DIR}/.. ; pwd)

if [ "x${JAVA_HOME}" = "x" ]; then
  echo "The JAVA_HOME is not setted."
  exit -1
fi

JAVA_BIN=${JAVA_HOME}/bin/java

APP_OPTS="-Xms50m -Xmx100m"
APP_CLASSPATH=${BASE_DIR}/*:${BASE_DIR}/conf/*:${BASE_DIR}/lib/*
${JAVA_BIN} ${APP_OPTS} -cp ${APP_CLASSPATH} org.ling.zookeeper.MultiZk
