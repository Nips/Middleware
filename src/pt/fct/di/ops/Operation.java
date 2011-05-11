package pt.fct.di.ops;

/**
 * This interface defines a generic operation
 * 
 * @author Andre_Goncalves@di
 */
public interface Operation{
	
	/**
	 * Gets the ID of the client
	 * @return
	 */
	public int getID();
	
	/**
	 * Gets the type of the operation - Update or Read?
	 */
	public int getType();
	
	/**
	 * Gets the column family of an operation
	 * @return
	 */
	public String getColumnFamily();
	
	/**
	 * Gets the row key of an operation
	 * @return
	 */
	public String getRowKey();
	
	/**
	 * Gets clients version vector
	 * @return
	 */
	public long[] getVersionVector();
	
	/**
	 * Verifies if an operation is valid.
	 * @throws OpException
	 */
	public boolean opValidation() throws OpException;
	
}
