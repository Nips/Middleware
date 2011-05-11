package pt.fct.di.db;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.util.Constants;

/**
 * HBase 0.90 client for middleware
 */
public class HBase extends DB{

	private final int Ok = 0;
    private final int ServerError=-1;
	private final int MAXVERSIONS = 3;
	private final String CONNECTION_RETRY_PROPERTY="cassandra.connectionretries";
	private final String OPERATION_RETRY_PROPERTY="cassandra.operationretries";

	public int _connectionRetries;
	public int _operationRetries;
	
	private Configuration _config = null;
	private HBaseAdmin _admin = null;
	private HTable _table = null;
//	private HTable[] _table = null; 
	
	private CharsetDecoder _decoder = null;
//	private CharsetDecoder[] _decoder = null;
	private boolean _debug = false;
//	private int nextInstance = 0;
	
	/**
	 * Create a table.
	 * @param tableName
	 * @param family
	 * @return An HTable instance for the created table.
	 * @throws IOException
	 */
	public HTable createTable(byte[] tableName, boolean defaultValues)
	throws IOException{
	  if(defaultValues) return createDefaultTable(tableName, new byte[][]{});
	  else return createTable(tableName, new byte[][]{});
	}
	
	/**
	 * Create a table.
	 * @param tableName
	 * @param family
	 * @return An HTable instance for the created table.
	 * @throws IOException
	 */
	public HTable createTable(byte[] tableName, byte[] family, boolean defaultValues)
	throws IOException{
	  if(defaultValues) return createDefaultTable(tableName, new byte[][]{family});
	  else return createTable(tableName, new byte[][]{family});
	}
	  
	 /**
	  * Create a table with default values.
	  * @param tableName
	  * @param families
	  * @return An HTable instance for the created table.
	  * @throws IOException
	  */
	 public HTable createDefaultTable(byte[] tableName, byte[][] families)
	 throws IOException {
	   HTableDescriptor desc = new HTableDescriptor(tableName);
	   for(byte[] family : families) {
	     desc.addFamily(new HColumnDescriptor(family, MAXVERSIONS,
	     HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
		 Integer.MAX_VALUE, HConstants.FOREVER, 
		 HColumnDescriptor.DEFAULT_BLOOMFILTER,
		 HConstants.REPLICATION_SCOPE_LOCAL));
	   }
	   _admin.createTable(desc);
	   return new HTable(getConfiguration(), tableName);
	}
	 
	 /**
	  * Create a table with default values.
	  * @param tableName
	  * @param families
	  * @return An HTable instance for the created table.
	  * @throws IOException
	  */
	 public HTable createTable(byte[] tableName, byte[][] families)
	 throws IOException {
	   HTableDescriptor desc = new HTableDescriptor(tableName);
	   for(byte[] family : families) {
		   //TODO:CHANGE NAMES TO THE RIGHT CORRESPONDENCES
	     desc.addFamily(new HColumnDescriptor(family, 
	    		 Integer.parseInt(_config.get("version", String.valueOf(MAXVERSIONS))),
	    		 _config.get("compression", HColumnDescriptor.DEFAULT_COMPRESSION), 
	    		 _config.getBoolean("inMemory", false), 
	    		 _config.getBoolean("", false), Integer.parseInt(_config.get("blocksize", String.valueOf(Integer.MAX_VALUE))), 
	    		 Integer.parseInt(_config.get("timetolive", String.valueOf(HConstants.FOREVER))),  
	    		 _config.get("bloomfilter", HColumnDescriptor.DEFAULT_BLOOMFILTER),
	    		 Integer.parseInt(_config.get("scope", String.valueOf(HConstants.REPLICATION_SCOPE_LOCAL)))));
	   }
	   _admin.createTable(desc);
	   return new HTable(getConfiguration(), tableName);
	}

	public Configuration getConfiguration()
	{
		return _config;
	}
	
	// Helper to translate byte[]'s to UTF8 strings
	private String utf8(byte[] buf) {
	  try {
		  synchronized(_decoder)
		  {
			  return _decoder.decode(ByteBuffer.wrap(buf)).toString();
		  }
	  } catch (CharacterCodingException e) {
	    return "[INVALID UTF-8]";
	  }
	}
	
	// Helper to translate byte[]'s to UTF8 strings
//	private String utf8(byte[] buf, int pos) {
//	  try {
//		  return _decoder[pos].decode(ByteBuffer.wrap(buf)).toString();
//	  } catch (CharacterCodingException e) {
//	    return "[INVALID UTF-8]";
//	  }
//	}
	  
	// Helper to translate strings to UTF8 bytes
	private byte[] bytes(String s) {
	  try {
	    return s.getBytes("UTF-8");
	  } catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	    return null;
	  }
	}
	
	@Override
	public void init() throws DBException
	{
		super.init();
		//System.out.println("hbaseINIT");

		String tableName = getProperties().getProperty("dbtablename", Constants.DEFAULT_DB_TABLE_NAME);
		String configFile = getProperties().getProperty("hbaseconfigfile");
		if(configFile == null) throw new DBException("Required property \"habse config file\" missing for HBaseClient");
		//System.out.println("Table "+tableName);
		_debug = Boolean.parseBoolean(getProperties().getProperty("debug","false"));
		
		_connectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
				Constants.DEFAULT_CONNECTION_RETRY_PROPERTY));
		_operationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
				Constants.DEFAULT_OPERATION_RETRY_PROPERTY));
		
		Exception connectexception=null;
		for (int retry=0; retry<_connectionRetries; retry++)
		{
			try {				
				//Loads configuration files by default (hbase-site.xml).
				_config = HBaseConfiguration.create();
				_config.addResource(new Path(configFile));
				if(_debug) System.out.println(_config.get("hbase.master"));
				
				//Helper object to convert Strings to byte[] and vice-versa.
//				_decoder = new CharsetDecoder[2];
//				_decoder[0] = Charset.forName("UTF-8").newDecoder();
//				_decoder[1] = Charset.forName("UTF-8").newDecoder();
				_decoder = Charset.forName("UTF-8").newDecoder();
				
				//Administration control over the DB.
				_admin = new HBaseAdmin(_config);
				
				//Creates a connection to this table in HBase DB
				//TODO: See exception thrown here
//				boolean exists = _admin.tableExists(tableName);
//				if(_debug) System.out.println("Table "+tableName+" exists? "+exists);
				if(!_admin.tableExists(tableName)){
					//System.err.println("TABLE NOT EXISTS");
					_table = createTable(bytes(tableName), bytes(Constants.DEFAULT_DB_FAMILY_COLUMN), true);
				}
				else
				{
					//System.err.println("TABLE EXISTS");
					_table = new HTable(_config, tableName);
				}
				
				//System.out.println(_table);
				//_table.flushCommits();
				
	            _table.setAutoFlush(false); //TODO: Change it to false when passing to testing mode 
	            _table.setWriteBufferSize(1024*1024*12);
				
//---------------------------------Parralel Instances Mode-----------------------------------//
				
//				_table = new HTable[2]; //TODO: Change 2 to some number
//				HTable table = null;
//				int initialInstance = 0;
//				
//				if(!_admin.tableExists(tableName)){
//					table = createTable(bytes(tableName), bytes(Constants.DEFAULT_DB_FAMILY_COLUMN), true);
//					table.setAutoFlush(false); //TODO: Change it to false when passing to testing mode 
//		            table.setWriteBufferSize(1024*1024*12);
//					_table[0] = table;
//					initialInstance = 1;
//				}   
//				
//				//Just Grab an instance for an exiting table 
//				for(int instance=initialInstance; instance<2; instance++)
//				{
//					table = new HTable(_config, tableName);
//					table.setAutoFlush(false); //TODO: Change it to false when passing to testing mode 
//		            table.setWriteBufferSize(1024*1024*12);
//					_table[instance] = table;
//				}
				
	            break;
				}catch (IOException e)
				{
					e.printStackTrace();
					connectexception=e;
				}
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException e)
				{}
		}
		if (connectexception!=null)
		{
			System.err.println("Unable to connect to hbase after "+_connectionRetries+" tries");
			System.err.println("Unable to connect to habse after "+_connectionRetries+" tries");
			throw new DBException(connectexception);
		}
	}
	
	@Override
	public void cleanup() throws DBException
	{
		super.cleanup();
		try {
			_table.flushCommits();
			_table.close();
//			for(int instance=0; instance<2; instance++)
//			{
//				_table[instance].flushCommits();
//				_table[instance].close();
//			}
			HConnectionManager.deleteConnection(_config, true);
			_admin = null;
			_config = null;
		} catch (IOException e) {
			DBException dbe = new DBException(e.getMessage());
			dbe.setStackTrace(e.getStackTrace());
			throw dbe;
		}
	}
	
//	private synchronized int getDBInstancePosition()
//	{
//		int instancePos = nextInstance ;
////		System.out.println("NInstance: "+instancePos);
//		nextInstance++;
//		if(nextInstance == 2) nextInstance = 0;
//		return instancePos;
//	}
//	
//	private synchronized void releaseDBInstance(int pos)
//	{
//		nextInstance = pos;
//	}
	
	@Override
	public ReadResult read(String column_family, String key, Set<String> fields,
			ConsistencyLevel level) {
		Exception errorexception=null;
		
		try
		{
			Get get = new Get(bytes(key));
			if(fields.size() == 0) get.addFamily((bytes(column_family)));
			else
				for(String field : fields)	get.addColumn(bytes(column_family), bytes(field));
			
//			if (_debug) {
//				System.err.println("Doing read from HBase columnfamily "+column_family);
//				System.err.println("Doing read for key: "+key);
//			}
			
			org.apache.hadoop.hbase.client.Result result = null;
			
//			synchronized(_table)
//			{
				result = _table.get(get);
//			}
			
			//---------Parallel mode---------//
//			int pos = getDBInstancePosition();
//			result = _table[pos].get(get);
//			releaseDBInstance(pos);
//			result = _table[1].get(get);
			
//			if (_debug)
//			{
//				System.out.print("READ: ");
//			}
			
			Map<String, String> results = new HashMap<String, String>();
			//NavigableMap<byte[], byte[]> map = result.getFamilyMap(bytes(column_family));
			
			for(KeyValue kv: result.raw())
			{
				results.put(utf8(kv.getQualifier()), utf8(kv.getValue()));
//				results.put(utf8(kv.getQualifier(),pos), utf8(kv.getValue(),pos));
				
//			    if (_debug)
//			    {
//			    	System.err.print("("+new String(kv.getQualifier())+"="+new String(kv.getValue())+")");
//			    }
			}
			
//			
//			if (_debug)
//			{
//			   System.out.println("");
//			}
			
			return new ReadResult(Ok, results); //Change to vector
		}catch (IOException e)
		{
			   errorexception=e;
		}
//		try
//		{
//			Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
			   
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.err);
		return new ReadResult(ServerError, errorexception.getMessage());
	}

	@Override
	public ScanResult scan(String column_family, String start_row_key,
			int recordcount, Set<String> fields, ConsistencyLevel level) {
		
		Exception errorexception=null;
		ResultScanner scanner = null;
		
		try
		{
			Scan scan = new Scan(bytes(start_row_key));
	        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
	        //We get back recordcount records
			scan.setCaching(recordcount);
			if(fields.size() == 0) scan.addFamily(bytes(column_family));
			else
				for(String field: fields) scan.addColumn(bytes(column_family), bytes(field));
			
			//get results
			synchronized(_table)
			{
				scanner = _table.getScanner(scan);
			}
			
//			int pos = getDBInstancePosition();
//			scanner = _table[pos].getScanner(scan);
//			releaseDBInstance(pos);
			
			Vector<Map<String,String>> results = new Vector<Map<String,String>>();
//			String key = null;
			HashMap<String, String> rowResult = null;
			int numResults = 0;
			for(org.apache.hadoop.hbase.client.Result rr = scanner.next(); rr != null; rr = scanner.next())
			{
				//get row key
//				key = utf8(rr.getRow());
//	            if (_debug)
//	            {
//	            	System.out.println("Got scan result for key: "+key);
//	            }
	            //add rowResult to result vector
	            rowResult = new HashMap<String, String>();
	            for(KeyValue kv : rr.raw())
	            {
	            	rowResult.put(utf8(kv.getQualifier()), utf8(kv.getValue()));
//				    if (_debug)
//				    {
//				    	System.out.print("("+new String(kv.getQualifier())+"="+new String(kv.getValue())+")");
//				    }
	            }
	            
	            results.add(rowResult);
	            numResults++;
	            if(numResults >= recordcount) break; //if hit recordcount, bail out
			} //done with row
			
			return new ScanResult(Ok, results);
			
		}catch (IOException e)
		{
	   		errorexception=e;
		}
//		try
//		{
//			Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
		finally{
			scanner.close();
		}
	   
		errorexception.printStackTrace();	
		errorexception.printStackTrace(System.out);
		return new ScanResult(ServerError, errorexception.getMessage());
	}

	@Override
	public UpdateResult put(String column_family, String key, Map<String, String> values, 
			long timestamp,ConsistencyLevel level) {
		
		Exception errorexception=null;
//		if(values.isEmpty()) errorexception = new DBException("DB - Update operation must have a collection of values non empty"); //Uncomment to add preconditions
//		else
//		{
			try
			{
				Put put = new Put(bytes(key));
				for(Map.Entry<String, String> entry : values.entrySet()) 
				{
//					if (_debug) {
//		                System.err.println("Adding field/value " + entry.getKey() + "/"+
//		                  entry.getValue() + " to put request");
//		            }
					
					put.add(bytes(column_family), bytes(entry.getKey()), timestamp, bytes(entry.getValue()));
				}
					
				synchronized(_table)
				{
					_table.put(put);
				}
				
	//			int pos = getDBInstancePosition();
	//			_table[pos].put(put);
	//			releaseDBInstance(pos);
				
//				if (_debug)
//				{
//					System.err.println("INSERT");
//				}
				
				//System.out.println("Exit Insert op DB");
	
				return new UpdateResult(Ok); //Be aware about timestamp for older operations
				
			}catch (IOException e)
			{
				   errorexception=e;
			}
	//		try
	//		{
	//			Thread.sleep(500);
	//		}
	//		catch (InterruptedException e)
	//		{
	//		}
//		}
	
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.err);
		return new UpdateResult(ServerError, errorexception.getMessage()); //Be aware about timestamp for older operations
	}
	
//	//TODO: See if lock is necessary
//	public Result delete(String column_family, String key, long timestamp, 
//			pt.fct.di.db.ConsistencyLevel level)
//	{
//		
//		Exception errorexception=null;
//		byte[] rowKey = bytes(key);	
//
//		try{
//			Delete delete = new Delete(rowKey);
//			delete.deleteFamily(bytes(column_family), timestamp);
//			
//			synchronized(_table)
//			{
//				_table.delete(delete);
//			}
//			
////			int pos = getDBInstancePosition();
////			_table[pos].delete(delete);
////			releaseDBInstance(pos);
//			
//			if (_debug)
//			{
//				System.out.println("INSERT");
//			}
//			
//			//System.out.println("Exit Insert op DB");
//
//			return new Result(Ok, "Success"); //Be aware about timestamp for older operations
//		
//		}catch (IOException e)
//		{
//		   errorexception=e;
//		}
////		try
////		{
////			Thread.sleep(500);
////		}
////		catch (InterruptedException e)
////		{
////		}
//			   
//		errorexception.printStackTrace();
//		errorexception.printStackTrace(System.out);
//		return new Result(ServerError, errorexception.getMessage()); //Be aware about timestamp for older operations
//	}
	
	/**
	 * Delete some fields of a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public UpdateResult delete(String column_family, String key, Set<String> fields, long timestamp, 
			pt.fct.di.db.ConsistencyLevel level)
	{
		Exception errorexception=null;
		
		//Delete intire row for a certain family
//		if(fields.isEmpty()) errorexception = new DBException("DB - Delete operation must have a collection of fields non empty"); //UnComment to add preconditions
//		else
//		{
			try{
			
				Delete delete = new Delete(bytes(key));
				for(String field : fields)
					delete.deleteColumn(bytes(column_family), bytes(field), timestamp);
				
				synchronized(_table)
				{
					_table.delete(delete);
				}
				
//				int pos = getDBInstancePosition();
//				_table[pos].delete(delete);
//				releaseDBInstance(pos);
				
//				if (_debug)
//				{
//					System.out.println("DELETE");
//				}
				
				//System.out.println("Exit Insert op DB");
	
				return new UpdateResult(Ok);
				
			}catch (IOException e)
			{
			   errorexception=e;
			}
//			try
//			{
//				Thread.sleep(500);
//			}
//			catch (InterruptedException e)
//			{
//			}
//		}
		   
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return new UpdateResult(ServerError, errorexception.getMessage());
	}
}
