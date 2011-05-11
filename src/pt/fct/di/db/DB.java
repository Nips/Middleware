package pt.fct.di.db;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;

/**
 * A layer for accessing a database. Each serverProxy will be given its own instance.
 * This class should be constructed using a no-argument constructor. Any argument-based initialization
 * should be done by init().
 * 
 * @author andre_goncalves@di
 *
 */
public abstract class DB {
	
	public class HBase {

	}

	/** 
	 * Properties for configuring this DB.
	 */
	Properties _p=new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_p=p;
	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _p; 
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		//System.out.println("Init");
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
	}
	
	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	public abstract ReadResult read(String column_family, String key, Set<String> fields, ConsistencyLevel level); 
	
	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result 
	 * will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a 
	 * discussion of error codes.
	 */
	public abstract ScanResult scan(String column_family, String start_row_key, int recordcount, 
			Set<String> fields, ConsistencyLevel level);
	
//	/**
//	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be 
//	 * written into the record with the specified record key, overwriting any existing values with the 
//	 * same field name.
//	 *
//	 * @param table The name of the table
//	 * @param key The record key of the record to write.
//	 * @param values A HashMap of field/value pairs to update in the record
//	 * @return Zero on success, a non-zero error code on error.  See this class's description for a 
//	 * discussion of error codes.
//	 */
//	public abstract Result update(String column_family, String key, HashMap<String,String> values,
//			long timestamp, ConsistencyLevel level);
	
	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be 
	 * written into the record with the specified record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a 
	 * discussion of error codes.
	 */
	public abstract UpdateResult put(String column_family, String key, Map<String,String> values,
			long timestamp, ConsistencyLevel level);

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a 
	 * discussion of error codes.
	 */
	public abstract UpdateResult delete(String column_family, String key, Set<String> fields, 
			long timestamp, ConsistencyLevel level);
	
}
