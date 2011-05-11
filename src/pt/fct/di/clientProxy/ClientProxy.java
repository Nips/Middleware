package pt.fct.di.clientProxy;

import java.util.HashMap;
import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import pt.fct.di.client.CException;
import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.clientProxy.comm.ClientCommFactory;
import pt.fct.di.clientProxy.comm.ClientComm;
import pt.fct.di.clientProxy.comm.UnknownCommException;
import pt.fct.di.clientProxy.service.ProxyService;
//import pt.fct.di.ops.Delete;
//import pt.fct.di.ops.Put;
//import pt.fct.di.ops.OpException;
//import pt.fct.di.ops.Read;
//import pt.fct.di.ops.Result;
//import pt.fct.di.ops.Scan;
import pt.fct.di.util.SystemProperties;

/**
 * ClientProxy represents the proxy associated to the client.
 * This simple class receives the operations invoked by the client and forwards them to the ProxyService class.
 * For each operation creates a special object that represents it and verifies its validity (object operations are 
 * in package pt.fct.di.Ops). In this way its possible to serialize the operation to send it to the server and have 
 * a more cleaned code.
 * 
 * @author andre_goncalves@di
 *
 */
public class ClientProxy {
	
	/**
	 * Properties to define client choices, e.g communication stack to use. 
	 */
	private Properties _p=new Properties();	
	
	private ProxyService _service;
	
	private boolean _debug;
	
	public ClientProxy()
	{
	}
	
	/**
	 * Set the properties for this communication channel.
	 */
	public void setProperties(Properties p)
	{
		_p=p;
		SystemProperties.init(_p);
	}

	/**
	 * Get the set of properties for this communication channel.
	 */
	public Properties getProperties()
	{
		return _p; 
	}
	
	public void init() throws CException
	{
		try
		{
			ClientComm comm = ClientCommFactory.newComm(_p);
			_service = new ProxyService(comm);
			_service.init();
			_debug = SystemProperties._debug;
			//System.out.println(_debug);
		}
		catch(UnknownCommException uce)
		{
			throw new CException(uce.getMessage());
		}
	}
	
	public void cleanup()
	{
		if(_debug) System.out.println("Closing ClientProxy...");
		_service.cleanup();
		if(_debug) System.out.println("ClientProxy closed...");
	}

	
	/**
	 * Read - Reads each value of all fields associated to a single row and column family.
	 * Return a map with pairs <field_name, field_value>.
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 * 
	 * @param column_family
	 * @param row_key
	 * @param result
	 * @param level
	 * @return code specifying if the execution was successful or generated and error.
	 * @throws CException
	 */
	public int read(String column_family, String row_key, HashMap<String,String> result,
			ConsistencyLevel level) throws CException
	{
		return this.read(column_family, row_key, new HashSet<String>(), result, level);
	}
	
	/**
	 * Read - Reads only the values of the fields, contained in the parameter "fields", associated to a 
	 * single row and column family. Returns an map with pairs <field_name, field_value>.
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 * 
	 * @param column_family
	 * @param row_key
	 * @param fields
	 * @param result
	 * @param level
	 * @return code specifying if the execution was successful or generated and error.
	 * @throws CException
	 */
	public int read(String column_family, String row_key, Set<String> fields, HashMap<String,String> result,
			ConsistencyLevel level) throws CException
	{
//		if(level == null)
//			throw new CException("Read - You must insert the desired consistency level");
//		if(level == ConsistencyLevel.ZERO || level == ConsistencyLevel.ANY)
//			throw new CException("Read - This operation is unsuported for Zero and ANY consistency levels");
		
//		try {
//			Read readOperation = new Read(column_family, row_key, fields, level);
//			readOperation.argsValidation();
			
			return _service.read(column_family, row_key, fields, result, level);
		
//		} catch (OpException e) {
//			CException ce = new CException(e.getMessage());
//			ce.setStackTrace(e.getStackTrace());
//			throw ce;
//		}
	}
	
	/**
	 * Scan - Makes a scan through an ordered list rows in a certain column family, starting in "start_row_key" and 
	 * limited in number by "record record". For each of the rows, reads the values of all associated fields and returns 
	 * a vector with a map containing pairs <field_name, field_value>. 
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 * 
	 * @param column_family
	 * @param start_row_key
	 * @param recordcount
	 * @param result
	 * @param level
	 * @return code specifying if the execution was successful or generated and error.
	 * @throws CException
	 */
	public int scan(String column_family, String start_row_key, int recordcount, 
			Vector<HashMap<String, String>> result, ConsistencyLevel level) throws CException
	{
		return this.scan(column_family, start_row_key, recordcount, new HashSet<String>(), result, level);
	}
	
	/**
	 * Scan - Makes a scan through an ordered list rows in a certain column family, starting in "start_row_key" and 
	 * limited in number by "record record". For each of the rows, reads the values of some associated fields, 
	 * specified in parameter "fields", and returns a vector with map containing pairs <field_name, field_value>.
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 *   
	 * @param column_family
	 * @param start_row_key
	 * @param recordcount
	 * @param fields
	 * @param result
	 * @param level
	 * @return code specifying if the execution was successful or generated and error
	 * @throws CException
	 */
	public int scan(String column_family, String start_row_key, int recordcount, Set<String> fields, 
			Vector<HashMap<String, String>> result, ConsistencyLevel level) throws CException
	{
//		if(level == null)
//			throw new CException("Scan - You must insert the desired consistency level");
//		if(level == ConsistencyLevel.ZERO || level == ConsistencyLevel.ANY)
//			throw new CException("Scan - This operation is unsuported for Zero and ANY consistency levels");
		
//		try{
//			Scan scanOperation = new Scan(column_family, start_row_key, recordcount, fields, level);
//			scanOperation.argsValidation();
			
			return _service.scan(column_family, start_row_key, recordcount, fields, result, level);
			
//			if(response.getCode() < 0) throw new CException(response.getMsg());
//			
//			Iterator<HashMap<String, String>> it = ((Vector<HashMap<String,String>>)response.getValues()).iterator();
//			while(it.hasNext())
//			{
//				HashMap<String, String> aux = new HashMap<String,String>();
//				for(Map.Entry<String, String> ent: it.next().entrySet())
//				{
//					aux.put(ent.getKey(), ent.getValue());
//				}
//				result.add(aux);
//			}
//			
//			return response.getCode();
		
//		}catch(OpException e) {
//			CException ce = new CException(e.getMessage());
//			ce.setStackTrace(e.getStackTrace());
//			throw ce;	
//		}			
	}
	
	/**
	 * Insert - Inserts a new row together with respective fields and values in a column family. Its fields are specified
	 * in parameter "values".
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 * 
	 * @param column_family
	 * @param row_key
	 * @param values
	 * @param level
	 * @return code specifying if the execution was successful or generated and error
	 * @throws CException
	 */
	public int put(String column_family, String row_key, HashMap<String, String> values, 
			ConsistencyLevel level) throws CException
	{
		if(level == null)
			throw new CException("Put - You must insert the desired consistency level");
		
//		try
//		{
//			Put insertOperation = new Put(column_family, row_key, values, level);
//			insertOperation.argsValidation();
			
			if(level == ConsistencyLevel.ZERO) return _service.putAsync(column_family, row_key, values);
			else return _service.putSync(column_family, row_key, values, level);
			
//			return 0;
			
//		} catch (OpException e) {
//			CException ce = new CException(e.getMessage());
//			ce.setStackTrace(e.getStackTrace());
//			throw ce;
//		}
	}
	
//	/**
//	 * Update - Updates an exiting row together with respective fields and values in a column family. New fields and values
//	 * can be added also. Its fields are specified in parameter "values".
//	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
//	 * 
//	 * @param column_family
//	 * @param row_key
//	 * @param values
//	 * @param level
//	 * @return code specifying if the execution was successful or generated and error
//	 * @throws CException
//	 */
//	public int update(String column_key, String row_key, HashMap<String, String> values, 
//			ConsistencyLevel level) throws CException
//	{
//		if(level == null)
//			throw new CException("Update - You must insert the desired consistency level");
//		
//		try
//		{
//			Update updateOperation = new Update(column_key, row_key, values, level);
//			updateOperation.argsValidation();
//		
//			if(level == ConsistencyLevel.ZERO) _service.updateAsync(updateOperation);
//			else _service.updateSync(updateOperation);
//			
//			return 0;
//			
//		} catch (OpException e) {
//			CException ce = new CException(e.getMessage());
//			ce.setStackTrace(e.getStackTrace());
//			throw ce;
//		}
//	}
	
	/**
	 * Delete - Deletes an entire row in a column family.
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 * 
	 * @param column_key
	 * @param row_key
	 * @param level
	 * @return
	 * @throws CException
	 */
	public int delete(String column_key, String row_key, ConsistencyLevel level) throws CException
	{
		return this.delete(column_key, row_key, new HashSet<String>(), level);
	}
	
	/**
	 * Delete - Deletes all fields specified in parameter fields for a single row and column family.
	 * In case of success returns code 0, otherwise returns a error code ( < 0 ).
	 * 
	 * @param column_key
	 * @param row_key
	 * @param fields
	 * @param level
	 * @return
	 * @throws CException
	 */
	public int delete(String column_key, String row_key, Set<String> fields, ConsistencyLevel level) throws CException
	{
		if(level == null)
			throw new CException("Remove - You must insert the desired consistency level");
		
//		try
//		{
//			Delete deleteOperation = new Delete(column_key, row_key, fields, level);
//			deleteOperation.argsValidation();
			
			if(level == ConsistencyLevel.ZERO)  return _service.removeAsync(column_key, row_key, fields);
			else return _service.removeSync(column_key, row_key, fields, level);
			
//			return 0;
//		
//		} catch (OpException e) {
//			CException ce = new CException(e.getMessage());
//			ce.setStackTrace(e.getStackTrace());
//			throw ce;
//		}
	}
}
