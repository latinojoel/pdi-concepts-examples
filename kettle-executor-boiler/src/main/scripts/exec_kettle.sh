#!/bin/sh

JAVA_OPTS="-Xms128m -Xmx512m"
BASEDIR=`dirname $0`

EXEC_JAR=$BASEDIR/kettle-executor-boiler-TRUNK-SNAPSHOT.jar
EXEC_CLASSNAME=org.itfactory.kettle.App
EXEC_CONFDIR=conf/
EXEC_CLASSPATH=$EXEC_CONFDIR:$EXEC_JAR

# Add PDI extra JARs to classpath
for f in `find $BASEDIR/lib -type f -name "*.jar"`
do
  EXEC_CLASSPATH=$EXEC_CLASSPATH:$f
done

# echo $EXEC_CLASSPATH

# Start flume agent
java $JAVA_OPTS -cp $EXEC_CLASSPATH $EXEC_CLASSNAME "${1+$@}"
