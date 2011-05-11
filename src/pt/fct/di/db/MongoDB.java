package pt.fct.di.db;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

//import com.mongodb.DBAddress;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.util.Constants;

public class MongoDB extends pt.fct.di.db.DB{
	
	static Random random=new Random();
	public static final int Ok=0;
	public static final int Error=-1;
	
	private final String CONNECTION_RETRY_PROPERTY="cassandra.connectionretries";
	private final String OPERATION_RETRY_PROPERTY="cassandra.operationretries";
	
	public int _connectionRetries;
	public int _operationRetries;
	
	private boolean _debug = false;
	private Mongo _mongo = null;
	private DB _db = null;
	private WriteConcern _writeConcern;
	
	@Override
	public void init() throws DBException
	{
		super.init();
		//System.out.println("hbaseINIT");
		
		String hosts=getProperties().getProperty("hosts");
		if (hosts==null)
		{
			throw new DBException("Required property \"hosts\" missing for MongoDBClient");
		}

		String tableName = getProperties().getProperty("dbtablename", Constants.DEFAULT_DB_TABLE_NAME);
		_debug = Boolean.parseBoolean(getProperties().getProperty("debug","false"));
		
		_connectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
				Constants.DEFAULT_CONNECTION_RETRY_PROPERTY));
		_operationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
				Constants.DEFAULT_OPERATION_RETRY_PROPERTY));
        
		String writeConcernType = getProperties().getProperty("mongodb.writeConcern");

        if ("none".equals(writeConcernType)) {
            _writeConcern = WriteConcern.NONE;
        } else if ("strict".equals(writeConcernType)) {
            _writeConcern = WriteConcern.SAFE;
        } else if ("normal".equals(writeConcernType)) {
            _writeConcern = WriteConcern.NORMAL;
        }
		
		String[] allhosts=hosts.split(",");
		String myhost=allhosts[random.nextInt(allhosts.length)];
		String[] dividedAddr = myhost.split(":"); 
		
		Exception connectexception=null;
		for (int retry=0; retry<_connectionRetries; retry++)
		{
			try {
				 _mongo = new Mongo(dividedAddr[0], Integer.parseInt(dividedAddr[1]));
				_db = _mongo.getDB(tableName);
			} catch (UnknownHostException e) {
				connectexception = e;
			} catch (MongoException e) {
				connectexception = e;
			}
		}
		if (connectexception!=null)
		{
			System.err.println("Unable to connect to MongoDB after "+_connectionRetries+" tries");
			throw new DBException(connectexception);
		}
	}
	
	@Override
	public void cleanup() throws DBException
	{
		System.out.println("Closing DB...");
		_mongo.close();
		System.out.println("DB Closed...");
	}
	
	@Override
	public ReadResult read(String column_family, String key, Set<String> fields,
			ConsistencyLevel level) 
	{
		
		Exception errorexception=null;
		try{
				//_db.requestStart();
				
				DBCollection collection = _db.getCollection(column_family);
				DBObject query = new BasicDBObject().append("_id", key);
				DBObject fieldsObj = new BasicDBObject();
				
				DBObject queryResult = null;
				if(fields == null || fields.isEmpty()) //Return all fields
					queryResult = collection.findOne(query);
				else
				{
					Iterator<String> it = fields.iterator();
					while(it.hasNext()) fieldsObj.put(it.next(), 1);
					queryResult = collection.findOne(query, fieldsObj);
				}
				
//				if (_debug)
//				{
//					System.out.print("READ: ");
//				}
				
				Map<String, String> result = new HashMap<String, String>();
	//				
	//				if(queryResult != null)
	//				{
	//					Iterator<String> it = fields.iterator();
	//					while(it.hasNext()){
	//						String field = it.next();
	//						System.out.println("field: "+field+" = "+queryResult.get(field));
	//					}
	//				}
	//				else System.out.println("Row does not exist...");
				
				if(queryResult != null)
				{
					//result.putAll(queryResult.toMap());
					Map<String,String> mapResult = (Map<String,String>)queryResult.toMap();
					Iterator<String> it = mapResult.keySet().iterator();
					String field = null;
					while(it.hasNext())
					{
						field = (String)it.next();
						if(field.equals("_id")) continue;
//						if(_debug) System.out.println("field = "+field+" value = "+mapResult.get(field));
						result.put(field, mapResult.get(field));
					}
				}
				
				//Thread.sleep(10000);
					
				return new ReadResult(Ok, result);
				
		}
		catch (Exception e)
		{
			errorexception=e;
		}
//			finally
//			{
//				if(_db != null) _db.requestDone();
//			}
//		try
//		{
//			Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
	
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return new ReadResult(Error, errorexception.getMessage());
	}

	@Override
	public ScanResult scan(String column_family, String start_row_key,
			int recordcount, Set<String> fields, ConsistencyLevel level) {
		
		Exception errorexception=null;
		try{
			//_db.requestStart();
			
			DBCollection collection = _db.getCollection(column_family);
			DBObject scanRange = new BasicDBObject().append("$gte", start_row_key);
			DBObject query = new BasicDBObject().append("_id", scanRange);
			DBCursor cursor = collection.find(query).limit(recordcount);
			
			Vector<Map<String,String>> results = new Vector<Map<String,String>>();
			Map mapResult = null;
			Iterator it = null;
			String field = null;
			while(cursor.hasNext())
			{
				mapResult = cursor.next().toMap();
				it = mapResult.entrySet().iterator();
				while(it.hasNext())
				{
					field = (String)it.next();
					//System.out.println("field = "+field+" value = "+mapResult.get(field));
				}
				results.add((HashMap<String,String>)mapResult);
			}
			
			return new ScanResult(Ok, results);
		}
		catch (Exception e)
		{
			errorexception=e;
		}
//			finally
//			{
//				if(_db != null) _db.requestDone();
//			}
//		try
//		{
//			Thread.sleep(500);
//		}
//		catch (InterruptedException e)
//		{
//		}
	
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return new ScanResult(Error, errorexception.getMessage());
	}

	@Override
	public UpdateResult put(String column_family, String key,
			Map<String, String> values, long timestamp, ConsistencyLevel level) {
		Exception errorexception=null;
		
//		System.out.println("A executar op....");
		
		
//		if(values.isEmpty()) errorexception = new DBException("DB - Put operation must have a collection of values non empty"); //Uncomment to add preconditions
//		else
//		{   
			try{
	//				_db.requestStart();
				
				/*if(_debug)
				{
					Iterator<Map.Entry<String, String>> it = values.entrySet().iterator();
					while(it.hasNext())
					{
						Map.Entry<String, String> entry = it.next();
						System.out.println(entry.getKey()+"="+entry.getValue());
					}
					System.out.println("done");
				}*/
				
				DBCollection collection = _db.getCollection(column_family);
				DBObject dbo = new BasicDBObject().append("_id", key);
				
				
				long nHintedRows = collection.getCount(dbo);
				//DBObject queryResult = collection.findOne(dbo);
				if(nHintedRows == 0){ //Insert new document
//					if(_debug) System.out.println("Create new row...");
					dbo.putAll(values);
					collection.insert(dbo, _writeConcern);
				}
				else{ //Update existing document
					DBObject u = new BasicDBObject();
					DBObject fieldsToSet = new BasicDBObject();
					
					fieldsToSet.putAll(values);
					u.put("$set", fieldsToSet);
					
					collection.setWriteConcern(_writeConcern);
					collection.update(dbo, u);
				}
	            
				// determine if record was inserted
		        DBObject errors = _db.getLastError();
	   
		        if(_debug) System.out.println(errors.get("ok")+" "+errors.get("err"));
	            boolean notRisedError = (Double) errors.get("ok") == 1.0 && errors.get("err") == null ? true : false;
	           
				if(notRisedError) return new UpdateResult(Ok);
				return new UpdateResult(Error, "Operation put couldn't be executed with success");				
			}
			catch (Exception e)
			{
				e.printStackTrace();
				errorexception=e;
			}
	//			finally
	//			{
	//				if(_db != null) _db.requestDone();
	//			}
	//		try
	//		{
	//			Thread.sleep(500);
	//		}
	//		catch (InterruptedException e)
	//		{
	//		}
//		}
	
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return new UpdateResult(Error, errorexception.getMessage());
	}

	@Override
	public UpdateResult delete(String column_family, String key, Set<String> fields,
			long timestamp, ConsistencyLevel level) {

		Exception errorexception=null;
		if(_debug) System.out.println("Starting delete operation for row: "+key);
		
//		if(fields.isEmpty()) errorexception = new DBException("DB - Delete operation must have a collection of fields non empty"); //UnComment to add preconditions
//		else
//		{  
			try{
				//_db.requestStart();
				
				DBCollection collection = _db.getCollection(column_family);
				DBObject dbo = new BasicDBObject().append("_id", key);
				if(_writeConcern.equals(WriteConcern.SAFE))
					dbo.put("$atomic", true);
				
				//Create object that will contain the inside fields to delete
				DBObject u = new BasicDBObject();
				DBObject fieldsToSet = new BasicDBObject();
				
				for(String field: fields)
				{
					fieldsToSet.put(field, 1);
					System.out.println("field: "+field);
				}
				u.put("$unset", fieldsToSet);
				
				collection.setWriteConcern(_writeConcern);
				collection.update(dbo, u);
				
				// determine if record was deleted
	            DBObject errors = _db.getLastError();
//	            if(_debug) System.out.println(errors.get("ok"));
	            boolean notRisedError = (Double) errors.get("ok") == 1.0 && errors.get("err") == null ? true : false;
	            
	            if(notRisedError) return new UpdateResult(Ok);
	            return new UpdateResult(Error, "Operation delete couldn't be executed with success");
			}
			catch (Exception e)
			{
				errorexception=e;
			}
	//			finally
	//			{
	//				if(_db != null) _db.requestDone();
	//			}
	//		try
	//		{
	//			Thread.sleep(500);
	//		}
	//		catch (InterruptedException e)
	//		{
	//		}
//		}
	
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return new UpdateResult(Error, errorexception.getMessage());
	}
}

