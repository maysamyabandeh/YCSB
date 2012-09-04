#point to the YCSB folder
YCSB = ../../../../../../../
#point to the universal lib of the cluster
LIB = ${YCSB}/../../lib
#the destination folder for generated class files
CLASSDIR=${YCSB}/build/classes/com/yahoo/ycsb/db/
#the required jar files
YCSBJAR=${YCSB}/build/ycsb.jar
OMIDJAR=${LIB}/omid-1.0-SNAPSHOT.jar
HADOOPJAR=${LIB}/hadoop-0.20.2-core.jar
HBASEJAR=${LIB}/hbase-0.92.0.jar
all:
	#ensure thatn the jar files exist
	ls ${YCSBJAR} ${OMIDJAR} ${HADOOPJAR} ${HBASEJAR} > /dev/null
	javac -classpath ${YCSBJAR}:${OMIDJAR}:${HADOOPJAR}:${HBASEJAR} TranHBaseClient.java
	mkdir -p ${CLASSDIR}
	mv *class ${CLASSDIR} 
	#you need to make ycsb.jar again
	rm  ${YCSB}/build/ycsb.jar
