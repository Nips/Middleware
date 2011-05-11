package pt.fct.di.db;
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

import pt.fct.di.ops.Result;
import pt.fct.di.util.Constants;

import java.io.IOException;

/**
 * HBase 0.90 client for middleware
 */
public class HBase extends DB{

	private final int Ok = 0;
    private final int ServerError=-1;
	private final int MAXVERSIONS = 3;
	private final String CONNECTION_RETRY_PROPERTY="cassandra.connectionretries";
//	private final String OPERATION_RETRY_PROPERTY="cassandra.operationretries";

	public int _connectionRetries;
	public int _operationRetries;
	
	private HBaseAdmin _admin = null;
	private Configuration _config;
	private HTable _table;
//	private Configuration _config[]; TODO: support for more instances
//	private HTable[] _table;
//	private HTable[] _updateTable;
//	private HTable _readTable;
	private int _dbInstances = 0;
	private int nextInstance = 0;
	
	private CharsetDecoder _decoder = null;
//	private CharsetDecoder[] _decoder;
	private boolean _debug = false;
	
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
//	   return new HTable(_config[0], tableName);
	   return new HTable(_config, tableName);
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
//	   Configuration conf = _config[0];
	   Configuration conf = _config;
	   for(byte[] family : families) {
		   //TODO:CHANGE NAMES TO THE RIGHT CORRESPONDENCES
	     desc.addFamily(new HColumnDescriptor(family,
	    		 Integer.parseInt(conf.get("version", String.valueOf(MAXVERSIONS))),
	    		 conf.get("compression", HColumnDescriptor.DEFAULT_COMPRESSION), 
	    		 conf.getBoolean("inMemory", false), 
	    		 conf.getBoolean("", false), Integer.parseInt(conf.get("blocksize", String.valueOf(Integer.MAX_VALUE))), 
	    		 Integer.parseInt(conf.get("timetolive", String.valueOf(HConstants.FOREVER))),  
	    		 conf.get("bloomfilter", HColumnDescriptor.DEFAULT_BLOOMFILTER),
	    		 Integer.parseInt(conf.get("scope", String.valueOf(HConstants.REPLICATION_SCOPE_LOCAL)))));
	   }
	   _admin.createTable(desc);
	   return new HTable(conf, tableName);
	}

//	public Configuration getConfiguration()
//	{
//		return _config;
//	}
	
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
	
//	// Helper to translate byte[]'s to UTF8 strings
//	private String utf8(byte[] buf, int instance) {
//	  try {
//	    return _decoder[instance].decode(ByteBuffer.wrap(buf)).toString();
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
	
	private void setHTableProperties(HTable table) throws IOException
	{
		table.setAutoFlush(false); //TODO: Change it to false when passing to testing mode 
		table.setWriteBufferSize(1024*1024*12);
	}
	
	@Override
	public void init() throws DBException
	{
		super.init();
		//System.out.println("hbaseINIT");

		String tableName = getProperties().getProperty("dbtablename", Constants.DEFAULT_DB_TABLE_NAME);
		String configFile = getProperties().getProperty("hbaseconfigfile");
		if(configFile == null) throw new DBException("Required property \"habse config file\" missing for HBaseClient");
		String dbinstances = getProperties().getProperty("dbinstances");
		if(dbinstances == null) throw new DBException("Required property \"db instances\" missing for HBaseClient");
		_dbInstances = Integer.parseInt(dbinstances);
		
		_debug = Boolean.parseBoolean(getProperties().getProperty("debug","false"));
		
		_connectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
				Constants.DEFAULT_CONNECTION_RETRY_PROPERTY));
		
//		_config = new Configuration[_dbInstances];
//		_updateTable = new HTable[_dbInstances];
//		_decoder = new CharsetDecoder[_dbInstances];
		
		Exception connectexception=null;
		for (int retry=0; retry<_connectionRetries; retry++)
		{
			try {
				//Loads configuration files by default (hbase-site.xml).
				_config = HBaseConfiguration.create();
				_config.addResource(new Path(configFile));
				
//				Configuration conf = null;
//				for(int instance=0; instance<_dbInstances; instance++)
//				{
//					conf = HBaseConfiguration.create();
//					conf.addResource(new Path(configFile));
//					_config[instance] = conf;
//				}
				
				//if(_debug) System.out.println(_config.get("hbase.master"));
				
				//Helper object to convert Strings to byte[] and vice-versa.
				_decoder = Charset.forName("UTF-8").newDecoder();
				
				//Administration control over the DB.
				_admin = new HBaseAdmin(_config);
//				_admin = new HBaseAdmin(_config[0]);
				
				//Creates a connection to this table in HBase DB
				//TODO: See exception thrown here
//				boolean exists = _admin.tableExists(tableName);
//				if(_debug) System.out.println("Table "+tableName+" exists? "+exists);
				
				//create a new table before getting instances
				if(!_admin.tableExists(tableName))
					_table = createTable(bytes(tableName), bytes(Constants.DEFAULT_DB_FAMILY_COLUMN), true);
				else
					_table = new HTable(_config, tableName);
				setHTableProperties(_table);

//---------------------------Parallel instances-----------------------------//				
//				HTable table = null;
//				int initialInstance = 0;
				
//				if(!_admin.tableExists(tableName)){
//					table = createTable(bytes(tableName), bytes(Constants.DEFAULT_DB_FAMILY_COLUMN), true);
//					setHTableProperties(table);
//					_updateTable[0] = table;
////					_decoder[0] = Charset.forName("UTF-8").newDecoder();
//					initialInstance = 1;
//				}   
//				
//				//Just Grab an instance for an exiting table 
//				for(int instance=initialInstance; instance<_dbInstances; instance++)
//				{
//					table = new HTable(_config, tableName);
////					table = new HTable(_config[instance], tableName);
//					setHTableProperties(table);
//					_updateTable[instance] = table;
////					_decoder[instance] = Charset.forName("UTF-8").newDecoder();
//				}
//				
//				//Connection to DB to deal only with reads
//				Configuration configRead = HBaseConfiguration.create();
//				configRead.addResource(new Path(configFile));
//				_readTable = new HTable(configRead, tableName);
//				setHTableProperties(_readTable);
				
				//System.out.println(_table);
				//_table.flushCommits();
				
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
			
//			for(int instance=0; instance<_dbInstances; instance++)
//			{
//				_updateTable[instance].flushCommits();
//				_updateTable[instance].close();
////				HConnectionManager.deleteConnection(_config[instance], true);
//			}
//			_readTable.flushCommits();
//			_readTable.close();
//			HConnectionManager.deleteAllConnections(true);
			HConnectionManager.deleteConnection(_config, true);
//			_updateTable = null;
//			_readTable = null;
			_admin = null;
			_config = null;
		} catch (IOException e) {
			DBException dbe = new DBException(e.getMessage());
			dbe.setStackTrace(e.getStackTrace());
			throw dbe;
		}
	}
	
	private synchronized int getDBInstancePosition()
	{
		int instancePos = nextInstance ;
//		System.out.println("NInstance: "+instancePos);
		nextInstance++;
		if(nextInstance == _dbInstances) nextInstance = 0;
		return instancePos;
	}
	
	private synchronized void releaseDBInstance(int pos)
	{
		nextInstance = pos;
	}
	
	@Override
	public Result read(String column_family, String key, Set<String> fields,
			ConsistencyLevel level) {
		Exception errorexception=null;
		
		try
		{
			Get get = new Get(bytes(key));
			if(fields == null || fields.size() == 0) get.addFamily((bytes(column_family)));
			else
				for(String field : fields)	get.addColumn(bytes(column_family), bytes(field));
			
			if (_debug) {
				System.err.println("Doing read from HBase columnfamily "+column_family);
				System.err.println("Doing read for key: "+key);
			}
			
			org.apache.hadoop.hbase.client.Result result = null;
			
//			synchronized(_table)
//			{
				result = _table.get(get);
//			}
			
//			int pos = getDBInstancePosition();
//			result = _table[pos].get(get);
//			releaseDBInstance(pos);
			
			if (_debug)
			{
				System.out.print("READ: ");
			}
			
			Map<String, String> results = new HashMap<String, String>();
			//NavigableMap<byte[], byte[]> map = result.getFamilyMap(bytes(column_family));
			
			for(KeyValue kv: result.raw())
			{
				results.put(utf8(kv.getQualifier()), utf8(kv.getValue()));
				//results.put(new String(kv.getQualifier(),"UTF-8"), new String(kv.getValue(),"UTF-8"));
				
//			    if (_debug)
//			    {
//			    	System.err.print("("+new String(kv.getQualifier())+"="+new String(kv.getValue())+")");
//			    }
			}
			
			//Isn't a better way? See if get of fields that doesn't appear in DB raises an exception
			/*for(String field: fields)
			{
				byte[] value = map.get(bytes(field));
				if(value != null) results.put(field, utf8(value));
				
				if (_debug)
				{
					System.out.print("("+new String(field)+"="+new String(utf8(value))+")");
				}
			}*/
			
			/*for(Map.Entry<byte[],byte[]> value: result.getFamilyMap(bytes(column_family)).entrySet()) 
			{
				results.put(utf8(value.getKey()), utf8(value.getValue()));
			      if (_debug)
			      {
			    	  System.out.print("("+new String(value.getKey())+"="+new String(value.getValue())+")");
			      }
			}*/
			
			if (_debug)
			{
			   System.out.println("");
			}
			   
			return new Result("read", Ok, results);
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
		return new Result("read", ServerError, errorexception.getMessage());
	}

	@Override
	public Result scan(String column_family, String start_row_key,
			int recordcount, Set<String> fields, ConsistencyLevel level) {
		
		Exception errorexception=null;
		ResultScanner scanner = null;
		
		try
		{
			Scan scan = new Scan(bytes(start_row_key));
	        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
	        //We get back recordcount records
			scan.setCaching(recordcount);
			if(fields == null || fields.size() == 0) scan.addFamily(bytes(column_family));
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
			
			Vector<HashMap<String,String>> results = new Vector<HashMap<String,String>>();
			String key = null;
			HashMap<String, String> rowResult = null;
			int numResults = 0;
			for(org.apache.hadoop.hbase.client.Result rr = scanner.next(); rr != null; rr = scanner.next())
			{
				//get row key
				key = utf8(rr.getRow());
//				key = utf8(rr.getRow(),pos);
	            if (_debug)
	            {
	            	System.out.println("Got scan result for key: "+key);
	            }
	            //add rowResult to result vector
	            rowResult = new HashMap<String, String>();
	            for(KeyValue kv : rr.raw())
	            {
	            	rowResult.put(utf8(kv.getQualifier()), utf8(kv.getValue()));
//	            	rowResult.put(utf8(kv.getQualifier(),pos), utf8(kv.getValue(),pos));
				    if (_debug)
				    {
				    	System.out.print("("+new String(kv.getQualifier())+"="+new String(kv.getValue())+")");
				    }
	            }
	            
	            results.add(rowResult);
	            numResults++;
	            if(numResults >= recordcount) break; //if hir recordcount, bail out
			} //done with row
			
			return new Result("scan", Ok, results);
			
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
		return new Result("scan", ServerError, errorexception.getMessage());
	}

	@Override
	public Result put(String column_family, String key, HashMap<String, String> values, 
			long timestamp,ConsistencyLevel level) {
		
		Exception errorexception=null;	
		try
		{
			Put put = new Put(bytes(key));
			for(Map.Entry<String, String> entry : values.entrySet()) 
			{
				if (_debug) {
	                System.err.println("Adding field/value " + entry.getKey() + "/"+
	                  entry.getValue() + " to put request");
	            }
				
				put.add(bytes(column_family), bytes(entry.getKey()), timestamp, bytes(entry.getValue()));
			}
				
			synchronized(_table)
			{
				_table.put(put);
			}
			
//			int pos = getDBInstancePosition();
//			_updateTable[pos].put(put);
//			releaseDBInstance(pos);
			
			if (_debug)
			{
				System.err.println("INSERT");
			}
			
			//System.out.println("Exit Insert op DB");

			return new Result("put", Ok, "Success", timestamp);
			
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
		return new Result("put", ServerError, errorexception.getMessage(), timestamp);
	}
	
	//TODO: See if lock is necessary
	public Result delete(String column_family, String key, long timestamp, 
			pt.fct.di.db.ConsistencyLevel level)
	{
		
		Exception errorexception=null;
		byte[] rowKey = bytes(key);	

		try{
			Delete delete = new Delete(rowKey);
			delete.deleteFamily(bytes(column_family), timestamp);
			
			synchronized(_table)
			{
				_table.delete(delete);
			}
			
//			int pos = getDBInstancePosition();
//			_updateTable[pos].delete(delete);
//			releaseDBInstance(pos);
			
			if (_debug)
			{
				System.out.println("INSERT");
			}
			
			//System.out.println("Exit Insert op DB");

			return new Result("delete", Ok, "Success", timestamp);
		
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
		errorexception.printStackTrace(System.out);
		return new Result("delete", ServerError, errorexception.getMessage(), timestamp);
	}
	
	/**
	 * Delete some fields of a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public Result delete(String column_family, String key, Set<String> fields, long timestamp, 
			pt.fct.di.db.ConsistencyLevel level)
	{
		//Delete intire row for a certain family
		if(fields == null || fields.isEmpty()) return this.delete(column_family, key, timestamp, level);
		
		Exception errorexception=null;
		try{
			
			Delete delete = new Delete(bytes(key));
			for(String field : fields)
				delete.deleteColumn(bytes(column_family), bytes(field), timestamp);
			
			synchronized(_table)
			{
				_table.delete(delete);
			}
			
//			int pos = getDBInstancePosition();
//			_updateTable[pos].delete(delete);
//			releaseDBInstance(pos);
			
			if (_debug)
			{
				System.out.println("DELETE");
			}
			
			//System.out.println("Exit Insert op DB");

			return new Result("delete", Ok, "Success", timestamp);
			
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
		errorexception.printStackTrace(System.out);
		return new Result("delete", ServerError, errorexception.getMessage(), timestamp);
	}
}
