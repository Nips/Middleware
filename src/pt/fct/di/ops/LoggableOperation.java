package pt.fct.di.ops;

import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.util.Pair;

public interface LoggableOperation extends Operation{
	
	/**
	 * Get family column family and key wrapped in a Pair object
	 * @return
	 */
	public Pair<String, String> getFamilyAndKey();
	
	
	/**
	 * Gets the fields or the values depending on the operation associated
	 * @return
	 */
	public FieldsOrValues getFieldsOrValues();
	
	/**
	 * Gets the consistency level, necessary to ensure in the middleware, of an operation.
	 * @return
	 */
	public ConsistencyLevel getConsistency();
	
	/**
	 * Method to update the fields relevant to the operation
	 * @param newFields
	 */
	public void updateFields(Set<String> newFields);
	
	/**
	 * Unset attribute FieldsOrValues
	 */
	public void unsetFieldsOrValues();
	
	/**
	 * Converts this operation to a log operation that accesses to remote values ;)
	 * @return
	 */
	public ILogOperation convertToLog();
	
	/**
	 * Converts this operation to a log operation that accesses to a remote database 
	 * @param _remotedb
	 * @return
	 */
	public ILogOperation convertToLog(DB _remotedb);
}
