package pt.fct.di.db;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Vector;
import java.util.Random;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.ConsistencyLevel;

import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.util.Constants;


/**
 * Cassandra 0.6 client for middleware
 */
public class Cassandra6 extends DB
{
	static Random random=new Random();
	public static final int Ok=0;
	public static final int Error=-1;

	public int ConnectionRetries;
	public int OperationRetries;

	public static final String CONNECTION_RETRY_PROPERTY="cassandra.connectionretries";
	public static final String OPERATION_RETRY_PROPERTY="cassandra.operationretries";
	
	TTransport[] transport;
	Cassandra.Client[] client;
	
	int nextInstance = 0;
	String _table = "";
	boolean _debug=false;

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance.
	 */
	@Override
	public void init() throws DBException
	{
		
		String hosts=getProperties().getProperty("hosts");
		if (hosts==null)
		{
			throw new DBException("Required property \"hosts\" missing for CassandraClient");
		}
		
		ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
				Constants.DEFAULT_CONNECTION_RETRY_PROPERTY));
		OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
				Constants.DEFAULT_OPERATION_RETRY_PROPERTY));

		_table = getProperties().getProperty("dbtablename",Constants.DEFAULT_DB_TABLE_NAME);
		_debug = Boolean.parseBoolean(getProperties().getProperty("debug","false"));

		String[] allhosts=hosts.split(",");
		//String myhost=allhosts[random.nextInt(allhosts.length)];
		//System.out.println("My host: ["+myhost+"]");
		//System.exit(0);

		Exception connectexception=null;

		client = new Cassandra.Client[allhosts.length];
		transport = new TTransport[allhosts.length];
		for (int retry=0; retry<ConnectionRetries; retry++)
		{
			TTransport tr = null;
			for(int instance = 0; instance < allhosts.length; instance++)
			{
				tr = new TSocket(allhosts[instance], 9160);
				client[instance] = new Cassandra.Client(new TBinaryProtocol(tr));
				transport[instance] = tr;
			
				try
				{
					tr.open();
					connectexception=null;
					//break;
				}
				catch (Exception e)
				{
					connectexception=e;
					break;
				}
			}
			if(connectexception == null) return;
		}
		
		if (connectexception!=null)
		{
			System.err.println("Unable to connect to "+allhosts+" after "+ConnectionRetries+" tries");
			throw new DBException(connectexception);
		}
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException
	{
		for(int instance = 0; instance < transport.length; instance++) transport[instance].close();
	}
	
	private synchronized int getDBInstancePosition()
	{
		int instancePos = nextInstance;
//		System.out.println("NInstance: "+instancePos);
		nextInstance++;
		if(nextInstance == client.length) nextInstance = 0;
		return instancePos;
	}
	
	private synchronized void releaseDBInstance(int pos)
	{
		nextInstance = pos;
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
	public ReadResult read(String column_family, String key, Set<String> fields, 
			pt.fct.di.db.ConsistencyLevel level)
	{
	   Exception errorexception=null;
	      
	   try
	   {
		   
		   SlicePredicate predicate;
		   if (fields.isEmpty())
		   {
		    
		      SliceRange sliceRange = new SliceRange();
		      sliceRange.setStart(new byte[0]);
		      sliceRange.setFinish(new byte[0]);
		      sliceRange.setCount(1000000);
	
		      predicate = new SlicePredicate();
		      predicate.setSlice_range(sliceRange);
		   }
		   else
		   {
		      Vector<byte[]> fieldlist=new Vector<byte[]>();
		      for (String s : fields)
		      {
		    	  fieldlist.add(s.getBytes("UTF-8"));
		      }
	
		      predicate = new SlicePredicate();
		      predicate.setColumn_names(fieldlist);
		   }
		   
		   ColumnParent parent = new ColumnParent(column_family);
		   
		   List<ColumnOrSuperColumn> results = null;
		   
		   int pos = getDBInstancePosition();
		   results = client[pos].get_slice(_table, key, parent, predicate, 
				   ConsistencyLevel.findByValue(level.getValue()));
		   releaseDBInstance(pos);
		   
		   
		   if (_debug)
		   {
		      System.out.print("READ: ");
		   }
		   
		   HashMap<String,String> result = new HashMap<String, String>();
		   
		   Column column = null;
		   for (ColumnOrSuperColumn oneresult : results)
		   {
		      column=oneresult.column;
		      result.put(new String(column.name),new String(column.value));
		      
		      if (_debug)
		      {
		    	  System.out.print("("+new String(column.name)+"="+new String(column.value)+")");
		      }
		   }
		   
		   if (_debug)
		   {
		      System.out.println("");
		   }
		   
		   return new ReadResult(Ok, result);
		}
		catch (Exception e)
		{
		   errorexception=e;
		}
	
//		try
//		{
//		   Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
		
	   errorexception.printStackTrace();
	   errorexception.printStackTrace(System.out);
	   return new ReadResult(Error, errorexception.getMessage());  
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
      public ScanResult scan(String column_family, String startkey, int recordcount, Set<String> fields,
    		  pt.fct.di.db.ConsistencyLevel level)
      {
    	  Exception errorexception=null;	 
	      
    	  try
    	  {
			SlicePredicate predicate;
			if (fields.isEmpty())
			{
			   SliceRange sliceRange = new SliceRange();
			   sliceRange.setStart(new byte[0]);
			   sliceRange.setFinish(new byte[0]);
			   sliceRange.setCount(1000000);
			   predicate = new SlicePredicate();
			   predicate.setSlice_range(sliceRange);
			}
			else
			{
				Vector<byte[]> fieldlist=new Vector<byte[]>();
				for (String s : fields)
				{
					fieldlist.add(s.getBytes("UTF-8"));
				}
				predicate = new SlicePredicate();
				predicate.setColumn_names(fieldlist);
			}
			ColumnParent parent = new ColumnParent(column_family);
			
			List<KeySlice> results = null;

			int pos = getDBInstancePosition();
			results = client[pos].get_range_slice(_table,parent,predicate,startkey,"",recordcount,
					ConsistencyLevel.findByValue(level.getValue()));
			releaseDBInstance(pos);
			
			if (_debug)
			{
				System.out.println("SCAN:");
			}
			
			Vector<Map<String,String>> result = new Vector<Map<String, String>>();
			
			Map<String, String> tuple = null;
			Column column = null;
			for (KeySlice oneresult : results)
			{
				tuple = new HashMap<String, String>();
				
				for (ColumnOrSuperColumn onecol : oneresult.columns)
				{
					column=onecol.column;
					tuple.put(new String(column.name),new String(column.value));
					
					if (_debug)
					{
						System.out.print("("+new String(column.name)+"="+new String(column.value)+")");
					}
				}
				
				result.add(tuple);
				if (_debug)
				{
					System.out.println();
				}
			}
	
			return new ScanResult(Ok, result);
		}
		catch (Exception e)
		{
		   errorexception=e;
		}
//		try
//		{
//		   Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
	   
	   errorexception.printStackTrace();
	   errorexception.printStackTrace(System.out);
	   return new ScanResult(Error, errorexception.getMessage());
	}	

//	/**
//	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
//	 * record key, overwriting any existing values with the same field name.
//	 *
//	 * @param table The name of the table
//	 * @param key The record key of the record to write.
//	 * @param values A HashMap of field/value pairs to update in the record
//	 * @return Zero on success, a non-zero error code on error
//	 */
//	public int update(String column_family, String key, HashMap<String,String> values, long timestamp, 
//			pt.fct.di.db.ConsistencyLevel level)
//	{
//		return insert(column_family, key, values, timestamp, level);
//	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the 
	 * specified record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public UpdateResult put(String column_family, String key, Map<String,String> values, long timestamp, 
			pt.fct.di.db.ConsistencyLevel level)
	{
		
//		System.out.println("MapSize db put: "+values.size());
		for(Map.Entry<String, String> entry : values.entrySet())
			System.out.println("Key: "+entry.getKey()+" Value: "+entry.getValue());
		
		Exception errorexception=null;
	  
		// insert data
		Map<String, Map<String, List<Mutation>>> mutation_map = 
			new HashMap<String, Map<String, List<Mutation>>>();	
		
		Map<String, List<Mutation>> batch_mutation=new HashMap<String, List<Mutation>>();
		Vector<Mutation> v=new Vector<Mutation>();
		
		batch_mutation.put(column_family,v);
		mutation_map.put(key, batch_mutation);

		try
		{
			Column col = null;
			ColumnOrSuperColumn c = null;
			Mutation mut = null;
			for (Map.Entry<String, String> field: values.entrySet())
			{
				col=new Column(field.getKey().getBytes("UTF-8"), field.getValue().getBytes("UTF-8"), timestamp);

				c=new ColumnOrSuperColumn();
				c.setColumn(col);
				c.unsetSuper_column();
				
				mut = new Mutation();
				mut.setColumn_or_supercolumn(c);
				mut.unsetDeletion();
				v.add(mut);
			}
			
			int pos = getDBInstancePosition();
			client[pos].batch_mutate(_table,
					mutation_map,
					ConsistencyLevel.findByValue(level.getValue()));
			releaseDBInstance(pos);
			
			if (_debug)
			{
				System.out.println("INSERT");
			}
			
			//System.out.println("Exit Insert op DB");

			return new UpdateResult(Ok); //Be aware about timestamp for older operations
		}
		catch (Exception e)
		{
		   errorexception=e;
		}
//		try
//		{
//		   Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
		
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return new UpdateResult(Error, errorexception.getMessage()); //Be aware about timestamp for older operations
	}

//	/**
//	 * Delete a record from the database. 
//	 *
//	 * @param table The name of the table
//	 * @param key The record key of the record to delete.
//	 * @return Zero on success, a non-zero error code on error
//	 */
//	public Result delete(String column_family, String key, long timestamp, pt.fct.di.db.ConsistencyLevel level)
//	{
//	   Exception errorexception=null;
//	   
//		try
//		{
//			int pos = getDBInstancePosition();
//			client[pos].remove(_table,key,new ColumnPath(column_family), timestamp,
//					ConsistencyLevel.findByValue(level.getValue()));
//			releaseDBInstance(pos);
//			
//			if (_debug)
//			{
//				System.out.println("DELETE");
//			}
//
//			return new Result(Ok, "Success");
//		}
//		catch (Exception e)
//		{
//		   errorexception=e;
//		}
////		try
////		{
////		   Thread.sleep(500);
////		}
////		catch (InterruptedException e)
////		{
////		}
//		
//	   errorexception.printStackTrace();
//	   errorexception.printStackTrace(System.out);
//	   return new Result(Error, errorexception.getMessage());
//	}
	
	/**
	 * Delete some fields of a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public UpdateResult delete(String column_family, String key, Set<String> fields, long timestamp, pt.fct.di.db.ConsistencyLevel level)
	{
//		if(fields.isEmpty()) return this.delete(column_family, key, timestamp, level);
		
		Exception errorexception=null;
		   
		try
		{
			Map<String, Map<String, List<Mutation>>> batchMutation = new HashMap<String,Map<String, List<Mutation>>>();
			Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
			Vector<Mutation> mutationVector = new Vector<Mutation>();
			
			mutationMap.put(column_family,mutationVector);
			batchMutation.put(key, mutationMap);
			
 			
			Mutation mutation = new Mutation();
			Deletion deletion = new Deletion();
			
			SlicePredicate predicate;
			
			Vector<byte[]> fieldlist=new Vector<byte[]>();
			for (String s : fields)
			{
				fieldlist.add(s.getBytes("UTF-8"));
			}

			predicate = new SlicePredicate();
			predicate.setColumn_names(fieldlist);
			
			deletion.setPredicate(predicate);
			deletion.setTimestamp(timestamp);
			deletion.unsetSuper_column();
			
			mutation.setDeletion(deletion);
			mutation.unsetColumn_or_supercolumn();
			
			
//			for(String field: fields)
//			{
//				d = new Deletion();
//				d.setTimestamp(timestamp);
//				d.setPredicate(new SlicePredicate(fields));
//				m = new Mutation();
//				
//				m.setDeletion(new Deletion());
//				mutation.put(column_family, m);
//			}
			
			int pos = getDBInstancePosition();
			client[pos].batch_mutate(_table, batchMutation, ConsistencyLevel.findByValue(level.getValue()));
			releaseDBInstance(pos);
			
//			if (_debug)
//	
//				System.out.println("DELETE");
//			}

			return new UpdateResult(Ok);
		}
		catch (Exception e)
		{
		   errorexception=e;
		}
//		try
//		{
//		   Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
		
	   errorexception.printStackTrace();
	   errorexception.printStackTrace(System.out);
	   return new UpdateResult(Error, errorexception.getMessage());
	}
}
