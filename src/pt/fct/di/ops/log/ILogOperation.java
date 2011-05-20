package pt.fct.di.ops.log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;
import java.util.Set;

import pt.fct.di.ops.LoggableOperation;

/**
 * This interface defines a generic operation inserted in the log
 */
public interface ILogOperation extends LoggableOperation, Comparable<ILogOperation>{
	
	public int getOpSeq();
	
	/**
	 * Sets new set of fields
	 * @param fields
	 */
	public void setFields(Set<String> fields);
	
	/**
	 * Sets a new timestamp for the operation
	 */
	public void setTS(long timestamp);
	
	/**
	 * Gets the set of fields for the operation
	 * @param fields
	 */
	public Set<String> getFields();
	
	/**
	 * Gets the timestamp set for the operation
	 */
	public long getTS();
	
	/**
	 * Serialize operation
	 * @param ois
	 * @throws IOException
	 */
	public void serialize(ObjectOutputStream oos) throws IOException;

	/**
	 * Deserialize operation
	 * @param ois
	 * @throws IOException
	 */
	public void deserialize(ObjectInputStream ois) throws IOException;
	
	public int compare(ILogOperation other);
}
