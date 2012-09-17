# Copyright (c) 2011 Yahoo! Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.

#point to the YCSB folder
YCSB = ../../../../../../../
#point to the universal lib of the cluster
LIB = ${YCSB}/../../lib
#the destination folder for generated class files
CLASSDIR=${YCSB}/build/classes/com/yahoo/ycsb/db/
#the required jar files
YCSBJAR=${YCSB}/build/ycsb.jar
OMIDJAR=${LIB}/omid-1.0-SNAPSHOT.jar
HADOOPJAR=${LIB}/hadoop-core-1.0.3.jar
HBASEJAR=${LIB}/hbase-0.94.1.jar
all:
	#ensure thatn the jar files exist
	ls ${YCSBJAR} ${OMIDJAR} ${HADOOPJAR} ${HBASEJAR} > /dev/null
	javac -classpath ${YCSBJAR}:${OMIDJAR}:${HADOOPJAR}:${HBASEJAR} HBaseClient.java
	mkdir -p ${CLASSDIR}
	mv *class ${CLASSDIR} 
	#you need to make ycsb.jar again
	rm  ${YCSB}/build/ycsb.jar
