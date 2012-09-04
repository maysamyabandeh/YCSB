/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import java.util.Properties;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative 
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * 
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul> 
 */
public class CoreWorkload extends Workload
{

	/**
	 * The name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY="table";

	/**
	 * The default name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY_DEFAULT="usertable";

	public static String table;


	/**
	 * The name of the property for the number of fields in a record.
	 */
	public static final String FIELD_COUNT_PROPERTY="fieldcount";
	
	/**
	 * Default number of fields in a record.
	 */
	public static final String FIELD_COUNT_PROPERTY_DEFAULT="10";

	int fieldcount;

	/**
	 * The name of the property for the length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY="fieldlength";
	
	/**
	 * The default length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY_DEFAULT="100";

	int fieldlength;

	/**
	 * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY="readallfields";
	
	/**
	 * The default value for the readallfields property.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT="true";

	boolean readallfields;

	/**
	 * The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY="writeallfields";
	
	/**
	 * The default value for the writeallfields property.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT="false";

	boolean writeallfields;


	/**
	 * The name of the property for the proportion of transactions that are reads.
	 */
	public static final String READ_PROPORTION_PROPERTY="readproportion";
	
	/**
	 * The default proportion of transactions that are reads.	
	 */
	public static final String READ_PROPORTION_PROPERTY_DEFAULT="0.95";

	/**
	 * The name of the property for the proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY="updateproportion";
	
	/**
	 * The default proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT="0.05";

	/**
	 * The name of the property for the proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY="insertproportion";
	
	/**
	 * The default proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY="scanproportion";
	
	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY_DEFAULT="0.0";
	
	/**
	 * The name of the property for the proportion of transactions that are read-modify-write.
	 */
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY="readmodifywriteproportion";
	
	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT="0.0";

    	/**
	 * The name of the property for the proportion of transactions that are scans.
	 */
    public static final String MULTI_UPDATE_PROPORTION_PROPERTY="multiupdateproportion";
    public static final String COMPLEX_PROPORTION_PROPERTY="complexproportion";
    public static final String MULTI_READ_PROPORTION_PROPERTY="multireadproportion";
    public static final String SCAN_WRITE_PROPORTION_PROPERTY="scanwriteproportion";
	
	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String MULTI_UPDATE_PROPORTION_PROPERTY_DEFAULT="0.0";
	public static final String COMPLEX_PROPORTION_PROPERTY_DEFAULT="0.0";
	public static final String MULTI_READ_PROPORTION_PROPERTY_DEFAULT="0.0";
	public static final String SCAN_WRITE_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
	
	/**
	 * The default distribution of requests across the keyspace
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

	/**
	 * The name of the property for the max scan length (number of records)
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY="maxscanlength";

	/**
	 * The default max scan length.
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT="1000";


	/**
	 * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY="scanlengthdistribution";
	
	/**
	 * The default max scan length.
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

        public static final String MAX_TRANSACTION_LENGTH_PROPERTY="maxtransactionlength";
        public static final String MAX_TRANSACTION_LENGTH_PROPERTY_DEFAULT="100";
        public static final String TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY="transactionlengthdistribution";
	public static final String TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

	/**
	 * The name of the property for the order to insert records. Options are "ordered" or "hashed"
	 */
	public static final String INSERT_ORDER_PROPERTY="insertorder";
	
	/**
	 * Default insert order.
	 */
	public static final String INSERT_ORDER_PROPERTY_DEFAULT="hashed";
	
	/**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  
  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  
  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  
  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";
	
	IntegerGenerator keysequence;

	DiscreteGenerator operationchooser;

	IntegerGenerator keychooser;
	//use it for status oracle partitions
	IntegerGenerator[] partitionedKeychoosers;
	Vector<SeqGenerator> keychoosersPool;
	IntegerGenerator partitionRandomSelector;
	IntegerGenerator globalTxnRndSelector;
	GlobalSeqGenerator globalSeqGenerator;
	int globalchance;//chance of a global txn %

	Generator fieldchooser;

	CounterGenerator transactioninsertkeysequence;
	
	IntegerGenerator scanlength;
    	IntegerGenerator transactionlength;

	DiscreteGenerator complexchooser;
	
	boolean orderedinserts;

	int recordcount;
	
	/**
	 * Initialize the scenario. 
	 * Called once, in the main client thread, before any operations are started.
	 */
	public void init(Properties p) throws WorkloadException
	{
		table = p.getProperty(TABLENAME_PROPERTY,TABLENAME_PROPERTY_DEFAULT);
		fieldcount=Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY,FIELD_COUNT_PROPERTY_DEFAULT));
		fieldlength=Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY,FIELD_LENGTH_PROPERTY_DEFAULT));
		double readproportion=Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,READ_PROPORTION_PROPERTY_DEFAULT));
		double updateproportion=Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,UPDATE_PROPORTION_PROPERTY_DEFAULT));
		double insertproportion=Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,INSERT_PROPORTION_PROPERTY_DEFAULT));
		double scanproportion=Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY,SCAN_PROPORTION_PROPERTY_DEFAULT));
		double readmodifywriteproportion=Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY,READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
		double multiupdateproportion=Double.parseDouble(p.getProperty(MULTI_UPDATE_PROPORTION_PROPERTY,MULTI_UPDATE_PROPORTION_PROPERTY_DEFAULT));
		double multireadproportion=Double.parseDouble(p.getProperty(MULTI_READ_PROPORTION_PROPERTY,MULTI_READ_PROPORTION_PROPERTY_DEFAULT));
		double scanwriteproportion=Double.parseDouble(p.getProperty(SCAN_WRITE_PROPORTION_PROPERTY,SCAN_WRITE_PROPORTION_PROPERTY_DEFAULT));
		double complexproportion=Double.parseDouble(p.getProperty(COMPLEX_PROPORTION_PROPERTY,COMPLEX_PROPORTION_PROPERTY_DEFAULT));
		recordcount=Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
		String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		int maxscanlength=Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY,MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
		String scanlengthdistrib=p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		
		complexchooser = new DiscreteGenerator();
		complexchooser.addValue(0.5, "READ");
		complexchooser.addValue(0.5, "WRITE");
		int maxtransactionlength=Integer.parseInt(p.getProperty(MAX_TRANSACTION_LENGTH_PROPERTY,MAX_TRANSACTION_LENGTH_PROPERTY_DEFAULT));
		String transactionlengthdistrib=p.getProperty(TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY,TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		
		int insertstart=Integer.parseInt(p.getProperty(INSERT_START_PROPERTY,INSERT_START_PROPERTY_DEFAULT));
		
		readallfields=Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY,READ_ALL_FIELDS_PROPERTY_DEFAULT));
		writeallfields=Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY,WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
		
		if (p.getProperty(INSERT_ORDER_PROPERTY,INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed")==0)
		{
			orderedinserts=false;
		}
		else
		{
			orderedinserts=true;
		}

		keysequence=new CounterGenerator(insertstart);
		operationchooser=new DiscreteGenerator();
		if (readproportion>0)
		{
			operationchooser.addValue(readproportion,"READ");
		}

		if (updateproportion>0)
		{
			operationchooser.addValue(updateproportion,"UPDATE");
		}

		if (insertproportion>0)
		{
			operationchooser.addValue(insertproportion,"INSERT");
		}
		
		if (scanproportion>0)
		{
			operationchooser.addValue(scanproportion,"SCAN");
		}
		
		if (readmodifywriteproportion>0)
		{
			operationchooser.addValue(readmodifywriteproportion,"READMODIFYWRITE");
		}

		if (multiupdateproportion>0)
		{
			operationchooser.addValue(multiupdateproportion,"MULTIUPDATE");
		}

		if (complexproportion>0)
		{
			operationchooser.addValue(complexproportion,"COMPLEX");
		}

		if (multireadproportion>0)
		{
			operationchooser.addValue(multireadproportion,"MULTIREAD");
		}

		if (scanwriteproportion>0)
		{
			operationchooser.addValue(scanwriteproportion,"SCANWRITE");
		}

		//read parameters related to global transactions
		globalchance=Integer.parseInt(p.getProperty("globalchance","-1"));
		System.out.println("Global Txn Chance: " + globalchance + "%");
		int partitions=Integer.parseInt(p.getProperty("partitions","1"));
		System.out.println("Number of partitions: " + partitions);
		partitionedKeychoosers = new IntegerGenerator[partitions];
		int partitionSize = recordcount / partitions;
		partitionRandomSelector=new UniformIntegerGenerator(0,partitions-1);
		globalTxnRndSelector=new UniformIntegerGenerator(0,100);//100% the total probability
		keychoosersPool = new Vector<SeqGenerator>();
		globalSeqGenerator = new GlobalSeqGenerator();

		transactioninsertkeysequence=new CounterGenerator(recordcount);
		if (requestdistrib.compareTo("uniform")==0)
		{
			keychooser=new UniformIntegerGenerator(0,recordcount-1);
			//initialize the key generators related to global transactions
			int end = 0;
			for (int i = 1; i <= partitions; i++) {
				int start = end;
				end = start + partitionSize;
				if (i == partitions)
					end = recordcount;
				partitionedKeychoosers[i-1] = new UniformIntegerGenerator(start,end-1);
			}
		}
		else if (requestdistrib.compareTo("zipfian")==0)
		{
			//it does this by generating a random "next key" in part by taking the modulus over the number of keys
			//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
			//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
			//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
			//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
			//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
			
			int opcount=Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
			int expectednewkeys=(int)(((double)opcount)*insertproportion*2.0); //2 is fudge factor
			
			keychooser=new ScrambledZipfianGenerator(recordcount+expectednewkeys);
			//initialize the key generators related to global transactions
			int end = 0;
			for (int i = 1; i <= partitions; i++) {
				int start = end;
				end = start + partitionSize;
				if (i == partitions)
					end = recordcount + expectednewkeys;
				partitionedKeychoosers[i-1] = new ScrambledZipfianGenerator(start,end-1);
			}
		}
		else if (requestdistrib.compareTo("latest")==0)
		{
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
			//initialize the key generators related to global transactions
			int end = 0;
			for (int i = 1; i <= partitions; i++) {
				int start = end;
				end = start + partitionSize;
				if (i == partitions)
					end = recordcount;
				//Here partitioning does not help since it is supposed to be around latest partition anyway
				partitionedKeychoosers[i-1] = new SkewedLatestGenerator(transactioninsertkeysequence);
			}
		}
		else if (requestdistrib.equals("hotspot")) 
		{
      double hotsetfraction = Double.parseDouble(p.getProperty(
          HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction = Double.parseDouble(p.getProperty(
          HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(0, recordcount - 1, 
          hotsetfraction, hotopnfraction);
			//initialize the key generators related to global transactions
			int end = 0;
			for (int i = 1; i <= partitions; i++) {
				int start = end;
				end = start + partitionSize;
				if (i == partitions)
					end = recordcount;
				partitionedKeychoosers[i-1] = new HotspotIntegerGenerator(start,end-1,
						hotsetfraction, hotopnfraction);
			}
    }
		else
		{
			throw new WorkloadException("Unknown distribution \""+requestdistrib+"\"");
		}

		fieldchooser=new UniformIntegerGenerator(0,fieldcount-1);
		
		if (scanlengthdistrib.compareTo("uniform")==0)
		{
			scanlength=new UniformIntegerGenerator(1,maxscanlength);
		}
		else if (scanlengthdistrib.compareTo("zipfian")==0)
		{
			scanlength=new ZipfianGenerator(1,maxscanlength);
		}
		else
		{
			throw new WorkloadException("Distribution \""+scanlengthdistrib+"\" not allowed for scan length");
		}

		
		if (transactionlengthdistrib.compareTo("uniform")==0)
		{
			transactionlength=new UniformIntegerGenerator(1,maxtransactionlength);
		}
		else if (transactionlengthdistrib.compareTo("zipfian")==0)
		{
			transactionlength=new ZipfianGenerator(1,maxtransactionlength);
		}
		else
		{
			throw new WorkloadException("Distribution \""+transactionlengthdistrib+"\" not allowed for transaction length");
		}
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
	public boolean doInsert(DB db, Object threadstate)
	{
		int keynum=keysequence.nextInt();
		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String dbkey="user"+keynum;
		String dbkey=makeKey(keynum);
		HashMap<String,String> values=new HashMap<String,String>();
		for (int i=0; i<fieldcount; i++)
		{
			String fieldkey="field"+i;
			String data=Utils.ASCIIString(fieldlength);
			values.put(fieldkey,data);
		}
		if (db.insert(table,dbkey,values) == 0)
			return true;
		else
			return false;
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
	public boolean doTransaction(DB db, Object threadState)
	{
		String op=operationchooser.nextString();

		if (op.compareTo("READ")==0)
		{
			doTransactionRead(db, threadState);
		}
		else if (op.compareTo("UPDATE")==0)
		{
			doTransactionUpdate(db, threadState);
		}
		else if (op.compareTo("INSERT")==0)
		{
			doTransactionInsert(db, threadState);
		}
		else if (op.compareTo("SCAN")==0)
		{
			doTransactionScan(db, threadState);
		}
 		else if (op.compareTo("MULTIUPDATE")==0)
		{
			doTransactionMultiUpdate(db, threadState);
		}
 		else if (op.compareTo("MULTIREAD")==0)
		{
			doTransactionMultiRead(db, threadState);
		}
 		else if (op.compareTo("COMPLEX")==0)
		{
			doTransactionComplex(db, threadState);
		}
 		else if (op.compareTo("SCANWRITE")==0)
		{
			doTransactionScanWrite(db, threadState);
		}

		else
		{
			doTransactionReadModifyWrite(db, threadState);
		}
		
		return true;
	}

	//return a thread-specific state
	@Override
	public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException
	{
		ThreadWorkloadState state = new ThreadWorkloadState();
		state.partition = partitionRandomSelector.nextInt();
		return state;
	}

	class ThreadWorkloadState {
		int intGeneratorIndex = -1;//use this to have a separate generator per each thread
		int partition = -1;//the assigned partition to this client
		boolean lastTxnWasGlobal = false;//use it to rerun a global txn
		@Override
		public String toString() {
			return "ThreadWorkloadState: partition " + partition;
		}
	}

	String format = null;
	//get a key id and covert it to string
	String makeKey(int keynum) {
		//return "user"+keynum;
		if (format == null) {
			int digits = Integer.toString(recordcount).length();
			format = "user%0" + digits + "d";
		}
		return String.format(format,keynum);
	}

	//A class to generate sequentail row ids
   //It is used to get the highest throughput out of Hbase
	class SeqGenerator extends IntegerGenerator {
		public int nextInt() {
			int lastint = lastInt();
			++lastint;
			if (lastint == recordcount)//if it is wrapped around
				lastint = 0;
			setLastInt(lastint);
			return lastint;
		}
	}

	//This class generates global row ids but still it sticks to the sequential order history
	class GlobalSeqGenerator extends IntegerGenerator {
		public int nextInt() {
			int index = keychooser.nextInt();
			index = index % keychoosersPool.size();
			//TODO: use a dedicated random generator
			SeqGenerator seqGenerator = keychoosersPool.elementAt(index);
			return seqGenerator.nextInt();
		}
	}

	/**
	 * randomly choose a partition key generator or the global key generator
	 */
		/*
	IntegerGenerator selectAKeyChooser(Object threadState) {
		if (partitionedKeychoosers.length == 1) {//no need for partitioning
			return keychooser;
		}
		if (tws.lastTxnWasGlobal) {//redo global txn
			tws.lastTxnWasGlobal = false;//reset it
			//System.out.println("GLOBAL");
			return keychooser;
		}
		if (globalTxnRndSelector.nextInt() < globalchance) {
			tws.lastTxnWasGlobal = true;
			//System.out.println("GLOBAL");
			return keychooser;
		}
			//System.out.println("LOCAL " + tws.partition);
		return partitionedKeychoosers[tws.partition];
		//return keychooser;
	};
		*/
	//In this implementation, since we use sequentail rows instead of random, we do not have to 
	//partition the keys based on status oracle partitions. Instread we partition them based on 
	//the number of clients, so that the load on HBase will be balanced. This automatically balances
	//the load on the status oracles as well (while keeping the traffic parition-local)
	IntegerGenerator selectAKeyChooser(Object threadState) {
		ThreadWorkloadState tws = (ThreadWorkloadState)threadState;
		if (tws.intGeneratorIndex == -1) {
			newSeqGenerator(tws);
		}
		IntegerGenerator localIntGenerator = keychoosersPool.elementAt(tws.intGeneratorIndex);
		if (partitionedKeychoosers.length == 1) {//no need for partitioning
			return localIntGenerator;
		}
		if (tws.lastTxnWasGlobal) {//redo global txn
			tws.lastTxnWasGlobal = false;//reset it
			//System.out.println("GLOBAL");
			return globalSeqGenerator;
		}
		if (globalTxnRndSelector.nextInt() < globalchance) {
			tws.lastTxnWasGlobal = true;
			return globalSeqGenerator;
		}
		return localIntGenerator;
		//return keychooser;
	};

	synchronized void newSeqGenerator(ThreadWorkloadState tws) {
		SeqGenerator intGenerator = new SeqGenerator();
		int randInt = keychooser.nextInt();
		intGenerator.setLastInt(randInt);
		int index = keychoosersPool.size();
		keychoosersPool.add(intGenerator);
		tws.intGeneratorIndex = index;
	}

	public void doTransactionRead(DB db, Object threadState)
	{
		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);
		//choose a random key
		int keynum;
		do
		{
			keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
		}
		while (keynum>transactioninsertkeysequence.lastInt());
		
		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String keyname="user"+keynum;
		String keyname=makeKey(keynum);

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.read(table,keyname,fields,new HashMap<String,String>());
	}
	
	public void doTransactionReadModifyWrite(DB db, Object threadState)
	{
		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);
		//choose a random key
		int keynum;
		do
		{
			keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
		}
		while (keynum>transactioninsertkeysequence.lastInt());
		
		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String keyname="user"+keynum;
		String keyname=makeKey(keynum);

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}
		
		HashMap<String,String> values=new HashMap<String,String>();

		if (writeallfields)
		{
		   //new data for all the fields
		   for (int i=0; i<fieldcount; i++)
		   {
		      String fieldname="field"+i;
		      String data=Utils.ASCIIString(fieldlength);		   
		      values.put(fieldname,data);
		   }
		}
		else
		{
		   //update a random field
		   String fieldname="field"+fieldchooser.nextString();
		   String data=Utils.ASCIIString(fieldlength);		   
		   values.put(fieldname,data);
		}

		//do the transaction
		
		long st=System.currentTimeMillis();

		db.read(table,keyname,fields,new HashMap<String,String>());
		
		db.update(table,keyname,values);

		long en=System.currentTimeMillis();
		
		Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int)(en-st));
	}
	
	public void doTransactionScanWrite(DB db, Object threadState)
	{
		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);
		//choose a random key
		int keynum;
		do
		{
			keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
		}
		while (keynum>transactioninsertkeysequence.lastInt());

		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String startkeyname="user"+keynum;
		String startkeyname=makeKey(keynum);
		
		//choose a random scan length
		int len=scanlength.nextInt();

		HashSet<String> fields=null;
		HashMap<String,String> values=new HashMap<String,String>();

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);

		   String data=Utils.ASCIIString(fieldlength);		   
		   values.put(fieldname,data);
		} else {
		   //new data for all the fields
		   for (int i=0; i<fieldcount; i++)
		   {
		      String fieldname="field"+i;
		      String data=Utils.ASCIIString(fieldlength);		   
		      values.put(fieldname,data);
		   }
		}

		db.scanWrite(table,startkeyname,len,fields,values);
	}

	
	public void doTransactionScan(DB db, Object threadState)
	{
		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);
		//choose a random key
		int keynum;
		do
		{
			keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
		}
		while (keynum>transactioninsertkeysequence.lastInt());

		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String startkeyname="user"+keynum;
		String startkeyname=makeKey(keynum);
		
		//choose a random scan length
		int len=scanlength.nextInt();

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.scan(table,startkeyname,len,fields,new Vector<HashMap<String,String>>());
	}

	public void doTransactionUpdate(DB db, Object threadState)
	{
		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);
		//choose a random key
		int keynum;
		do
		{
			keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
		}
		while (keynum>transactioninsertkeysequence.lastInt());

		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String keyname="user"+keynum;
		String keyname=makeKey(keynum);

		HashMap<String,String> values=new HashMap<String,String>();

		if (writeallfields)
		{
		   //new data for all the fields
		   for (int i=0; i<fieldcount; i++)
		   {
		      String fieldname="field"+i;
		      String data=Utils.ASCIIString(fieldlength);		   
		      values.put(fieldname,data);
		   }
		}
		else
		{
		   //update a random field
		   String fieldname="field"+fieldchooser.nextString();
		   String data=Utils.ASCIIString(fieldlength);		   
		   values.put(fieldname,data);
		}

		db.update(table,keyname,values);
	}

	public void doTransactionMultiRead(DB db, Object threadState)
	{
		//choose a random scan length
		int len=transactionlength.nextInt();

		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);

		List<String> keys = new ArrayList<String>(len);
		for (int i = 0; i < len; i++) {
			//choose a random key
			int keynum;
			do
			{
				keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
			}
			while (keynum>transactioninsertkeysequence.lastInt());

			if (!orderedinserts)
			{
				keynum=Utils.hash(keynum);
			}
			//String keyname="user"+keynum;
			String keyname=makeKey(keynum);
			keys.add(keyname);
		}

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.readMulti(table,keys,fields,new HashMap<String,Map<String,String>>());
	}

	public void doTransactionComplex(DB db, Object threadState)
	{
		//choose a random scan length
		int len=transactionlength.nextInt();
		//to enable keys that are likely to be limited to a partition
		IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);

		List<String> readKeys = new ArrayList<String>();
		List<String> writeKeys = new ArrayList<String>();
		for (int i = 0; i < len; i++) {
		    //choose a random key
		    int keynum;
		    do
			{
				keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
			}
		    while (keynum>transactioninsertkeysequence.lastInt());
		    
		    if (!orderedinserts)
			{
			    keynum=Utils.hash(keynum);
			}
		    if (complexchooser.nextString().compareTo("READ") == 0) {
			    //readKeys.add("user"+keynum);
			    readKeys.add(makeKey(keynum));
		    } else {
			    //writeKeys.add("user"+keynum);
			    writeKeys.add(makeKey(keynum));
		    }
		}		    

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		HashMap<String,String> values=new HashMap<String,String>();

		if (writeallfields)
		{
		   //new data for all the fields
		   for (int i=0; i<fieldcount; i++)
		   {
		      String fieldname="field"+i;
		      String data=Utils.ASCIIString(fieldlength);		   
		      values.put(fieldname,data);
		   }
		}
		else
		{
		   //update a random field
		   String fieldname="field"+fieldchooser.nextString();
		   String data=Utils.ASCIIString(fieldlength);		   
		   values.put(fieldname,data);
		}

		db.complex(table,readKeys,fields, new HashMap<String,Map<String,String>>(), writeKeys, values);
	}
	public void doTransactionMultiUpdate(DB db, Object threadState)
	{
	    //choose a random scan length
	    int len=transactionlength.nextInt();
		 //to enable keys that are likely to be limited to a partition
		 IntegerGenerator selectedKeychooser = selectAKeyChooser(threadState);

		List<String> keys = new ArrayList<String>(len);
		for (int i = 0; i < len; i++) {
		    //choose a random key
		    int keynum;
		    do
			{
				keynum=selectedKeychooser.nextInt();
			//System.out.println("KEY: " + keynum + " " + threadState);
			}
		    while (keynum>transactioninsertkeysequence.lastInt());
		    
		    if (!orderedinserts)
			{
			    keynum=Utils.hash(keynum);
			}
		    //keys.add("user"+keynum);
		    keys.add(makeKey(keynum));
		}		    

		HashMap<String,String> values=new HashMap<String,String>();

		if (writeallfields)
		{
		   //new data for all the fields
		   for (int i=0; i<fieldcount; i++)
		   {
		      String fieldname="field"+i;
		      String data=Utils.ASCIIString(fieldlength);		   
		      values.put(fieldname,data);
		   }
		}
		else
		{
		   //update a random field
		   String fieldname="field"+fieldchooser.nextString();
		   String data=Utils.ASCIIString(fieldlength);		   
		   values.put(fieldname,data);
		}

		db.updateMulti(table,keys,values);
	}

	public void doTransactionInsert(DB db, Object threadState)
	{
		//choose the next key
		int keynum=transactioninsertkeysequence.nextInt();
		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		//String dbkey="user"+keynum;
		String dbkey=makeKey(keynum);
		
		HashMap<String,String> values=new HashMap<String,String>();
		for (int i=0; i<fieldcount; i++)
		{
			String fieldkey="field"+i;
			String data=Utils.ASCIIString(fieldlength);
			values.put(fieldkey,data);
		}
		db.insert(table,dbkey,values);
	}
}
