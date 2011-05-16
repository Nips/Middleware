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

package pt.fct.di.db;

import java.io.IOException;
import java.util.*;
//import java.util.HashMap;
//import java.util.Properties;
//import java.util.Set;
//import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.io.Cell;
//import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;

/**
 * HBase client for YCSB framework
 */
public class HBase extends DB
{
    // BFC: Change to fix broken build (with HBase 0.20.6)
    //private static final Configuration config = HBaseConfiguration.create();
	private final String DEFAULT_TABLE_NAME = "usertable";
    private static final Configuration config = HBaseConfiguration.create();

    public boolean _debug=false;

    public String _table="";
    public HTable _hTable=null;
//    public String _columnFamily="";
//    public byte _columnFamilyBytes[];

    public static final int Ok=1; //To differentiate with NOOp result
    public static final int ServerError=-1;
    public static final int HttpError=-2;
    public static final int NoMatchingRecord=-3;

    public static final Object tableLock = new Object();

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		if ( (getProperties().getProperty("debug")!=null) &&
				(getProperties().getProperty("debug").compareTo("true")==0) )
		{
		    _debug=true;
	    }


		config.addResource(new Path("conf/hbase-site.xml"));
//	    _columnFamily = getProperties().getProperty("columnfamily");
//	    if (_columnFamily == null) 
//	    {
//		    System.err.println("Error, must specify a columnfamily for HBase table");
//		    throw new DBException("No columnfamily specified");
//	    }
//      _columnFamilyBytes = Bytes.toBytes(_columnFamily);

    }

    /**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
    public void cleanup() throws DBException
    {
        try {
            if (_hTable != null) {
                _hTable.flushCommits();
            }
        } catch (IOException e) {
            throw new DBException(e);
        }
    }

    public void getHTable(String table) throws IOException
    {
        synchronized (tableLock) {
            _hTable = new HTable(config, table);
            //2 suggestions from http://ryantwopointoh.blogspot.com/2009/01/performance-of-hbase-importing.html
            _hTable.setAutoFlush(true); //Change setAutoFlush to false in testing mode
            _hTable.setWriteBufferSize(1024*1024*12);
            //return hTable;
        }

    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public ReadResult read(String columnFamily, String key, Set<String> fields, ConsistencyLevel level)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(DEFAULT_TABLE_NAME)) {
            _hTable = null;
            try 
            {
                getHTable(DEFAULT_TABLE_NAME);
                _table = DEFAULT_TABLE_NAME;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return new ReadResult(ServerError);
            }
        }

        Result r = null;
        try
        {
	    if (_debug) {
		System.out.println("Doing read from HBase columnfamily "+columnFamily);
		System.out.println("Doing read for key: "+key);
	    }
            Get g = new Get(Bytes.toBytes(key));
          if (fields == null) {
            g.addFamily(Bytes.toBytes(columnFamily));
          } else {
            for (String field : fields) {
              g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field));
            }
          }
            r = _hTable.get(g);
        }
        catch (IOException e)
        {
            System.err.println("Error doing get: "+e);
            return new ReadResult(ServerError);
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...need to understand HBase concurrency model better
            return new ReadResult(ServerError);
        }

        Map<String,String> result = new HashMap<String,String>(r.size());
        for (KeyValue kv : r.raw()) {
        	result.put(
        			Bytes.toString(kv.getQualifier()),
        			Bytes.toString(kv.getValue()));
        	if (_debug) {
        		System.out.println("Result for field: "+Bytes.toString(kv.getQualifier())+
        				" is: "+Bytes.toString(kv.getValue()));
        	}

        }
        return new ReadResult(Ok,result);
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
    public ScanResult scan(String columnFamily, String startkey, int recordcount, 
			Set<String> fields, ConsistencyLevel level)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(DEFAULT_TABLE_NAME)) {
            _hTable = null;
            try 
            {
                getHTable(DEFAULT_TABLE_NAME);
                _table = DEFAULT_TABLE_NAME;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return new ScanResult(ServerError);
            }
        }

        Scan s = new Scan(Bytes.toBytes(startkey));
        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
        //We get back recordcount records
        s.setCaching(recordcount);

        //add specified fields or else all fields
        if (fields == null)
        {
            s.addFamily(Bytes.toBytes(columnFamily));
        }
        else
        {
            for (String field : fields)
            {
                s.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(field));
            }
        }

        //get results
        Vector<Map<String,String>> result = new Vector<Map<String,String>>();
        ResultScanner scanner = null;
        try {
            scanner = _hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                //get row key
                String key = Bytes.toString(rr.getRow());
                if (_debug)
                {
                    System.out.println("Got scan result for key: "+key);
                }

                HashMap<String,String> rowResult = new HashMap<String, String>();

                for (KeyValue kv : rr.raw()) {
                  rowResult.put(
                      Bytes.toString(kv.getQualifier()),
                      Bytes.toString(kv.getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult);
                numResults++;
                if (numResults >= recordcount) //if hit recordcount, bail out
                {
                    break;
                }
            } //done with row

        }

        catch (IOException e) {
            if (_debug)
            {
                System.out.println("Error in getting/parsing scan result: "+e);
            }
            return new ScanResult(ServerError);
        }

        finally {
            scanner.close();
        }

        return new ScanResult(Ok, result);
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public UpdateResult put(String columnFamily, String key, Map<String,String> values,
			long timestamp, ConsistencyLevel level)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(DEFAULT_TABLE_NAME)) {
            _hTable = null;
            try 
            {
                getHTable(DEFAULT_TABLE_NAME);
                _table = DEFAULT_TABLE_NAME;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return new UpdateResult(ServerError);
            }
        }


        if (_debug) {
            System.out.println("Setting up put for key: "+key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, String> entry : values.entrySet())
        {
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/"+
                  entry.getValue() + " to put request");
            }	       
            p.add(Bytes.toBytes(columnFamily),Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
        }

        try 
        {
            _hTable.put(p);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing put: "+e);
            }
            return new UpdateResult(ServerError);
        }
        catch (ConcurrentModificationException e) 
        {
            //do nothing for now...hope this is rare
            return new UpdateResult(ServerError);
        }

        return new UpdateResult(Ok);
    }

//    /**
//     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
//     * record key.
//     *
//     * @param table The name of the table
//     * @param key The record key of the record to insert.
//     * @param values A HashMap of field/value pairs to insert in the record
//	 * @return Zero on success, a non-zero error code on error
//	 */
//	public int insert(String table, String key, HashMap<String,String> values)
//    {
//        return update(table,key,values);
//    }

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public UpdateResult delete(String columnFamily, String key, Set<String> fields, 
			long timestamp, ConsistencyLevel level)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(DEFAULT_TABLE_NAME)) {
            _hTable = null;
            try 
            {
                getHTable(DEFAULT_TABLE_NAME);
                _table = DEFAULT_TABLE_NAME;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return new UpdateResult(ServerError);
            }
        }

        if (_debug) {
            System.out.println("Doing delete for key: "+key);
        }

        Delete d = new Delete(Bytes.toBytes(key));
		for(String field : fields)
			d.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field));
        try 
        {
            _hTable.delete(d);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing delete: "+e);
            }
            return new UpdateResult(ServerError);
        }

        return new UpdateResult(Ok);
    }

    public static void main(String[] args)
    {
        if (args.length!=3)
        {
            System.out.println("Please specify a threadcount, columnfamily and operation count");
            System.exit(0);
        }

        final int keyspace=10000; //120000000;

        final int threadcount=Integer.parseInt(args[0]);	 

        final String columnfamily=args[1];


        final int opcount=Integer.parseInt(args[2])/threadcount;

        Vector<Thread> allthreads=new Vector<Thread>();

        for (int i=0; i<threadcount; i++)
        {
            Thread t=new Thread() 
            {
                public void run()
                {
                    try
                    {
                        Random random=new Random();
                        Properties props=new Properties();
                        props.setProperty("dbname", "pt.fct.di.db.HBase");
                        props.setProperty("columnfamily",columnfamily);
                        props.setProperty("debug","true");

                        DB cli= DBFactory.newDB(props);
                        cli.init();

                        //HashMap<String,String> result=new HashMap<String,String>();

                        long accum=0;

                        for (int i=0; i<opcount; i++)
                        {
                            int keynum=random.nextInt(keyspace);
                            String key="user"+keynum;
                            long st=System.currentTimeMillis();
                            int rescode;
                            
                            HashMap hm = new HashMap();
                            hm.put("field1","value1");
                            hm.put("field2","value2");
                            hm.put("field3","value3");
                            rescode=cli.put("data",key,hm,System.currentTimeMillis(),ConsistencyLevel.ONE).getCode();
                            System.out.println("Return code: "+rescode);
                            HashSet<String> s = new HashSet();
                            s.add("field1");
                            s.add("field2");
                            
                            cli.read("data", key, s, ConsistencyLevel.ONE);
//                            for(Map.Entry<String, String> entry : r.getValues().entrySet())
//                            	System.out.println("Field: "+entry.getKey()+", Value: "+entry.getValue());
                            
                            rescode=cli.delete("data",key,s,System.currentTimeMillis(),ConsistencyLevel.ONE).getCode();
                            System.out.println("Return code: "+rescode);
                            cli.read("data", key, s, ConsistencyLevel.ONE);
//                            System.out.println("Size of results: "+r.getValues().size());
                            
//                            HashSet<String> scanFields = new HashSet<String>();
//                            scanFields.add("field1");
//                            scanFields.add("field3");
//                            Vector<HashMap<String,String>> scanResults = new Vector<HashMap<String,String>>();
//                            rescode = cli.scan("table1","user2",20,null,scanResults);
                           
                            long en=System.currentTimeMillis();

                            accum+=(en-st);

//                            if (rescode!=Ok)
//                            {
//                                System.out.println("Error "+rescode+" for "+key);
//                            }

                            if (i%1==0)
                            {
                                System.out.println(i+" operations, average latency: "+(((double)accum)/((double)i)));
                            }
                        }

                        //System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
                        //System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            };
            allthreads.add(t);
        }

        long st=System.currentTimeMillis();
        for (Thread t: allthreads)
        {
            t.start();
        }

        for (Thread t: allthreads)
        {
            try
            {
                t.join();
            }
            catch (InterruptedException e)
            {
            }
        }
        long en=System.currentTimeMillis();

        System.out.println("Throughput: "+((1000.0)*(((double)(opcount*threadcount))/((double)(en-st))))+" ops/sec");

    }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/