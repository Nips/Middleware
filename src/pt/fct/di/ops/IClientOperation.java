package pt.fct.di.ops;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;

/**
 * This interface defines a generic operation done by a client
 * 
 * @author Andre_Goncalves@di
 */
public interface IClientOperation extends Operation{
	
	/**
	 * Register the client's ID.
	 * @param id
	 */
	public void setID(int id);
	
	/**
	 * Sets the Sequence Number of this operation.
	 * This number must be unique to each operation, created by a client, to distinguish them.
	 * In case this operation is a Result the Sequence Number is given by the operation that generated it.
	 */
	public void setOpSeq(int seq);
	
	public void setVersionVector(long[] vector);
	
	public void setConsistency(ConsistencyLevel cl);
	
	/**
	 * Gets the Sequence Number of the operation.
	 * @return
	 */
	public int getOpSeq();
	
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
	 * Serialize an operation
	 * @return
	 * @throws IOException
	 */
	public byte[] serialize() throws IOException;
	
	/**
	 * Serialize an operation to the specified inputstream
	 * @return
	 * @throws IOException
	 */
	public void serialize(ObjectOutputStream ois) throws IOException;
	
	/**
	 * Deserialize an operation
	 * 
	 * @param buffer Byte array with operation content
	 * @return
	 * @throws IOException
	 */
	public void deserialize(byte[] buffer) throws IOException, ClassNotFoundException;
	
	/**
	 * Deserialize an operation
	 * 
	 * @param ois ObjectInputStream to read operation content
	 * @return
	 * @throws IOException
	 */
	public void deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException;
	
	/**
	 * Convert an operation object to a byte array
	 * @return
	 * @throws OpException
	 * @throws IOException
	 */
	public byte[] toByteArray() throws OpException;
	
	/**
	 * Method to update the fields relevant to the operation
	 * @param newFields
	 */
	public void updateFields(Set<String> newFields);
	
	/**
	 * Verifies if the arguments of an operation are valid.
	 * @throws OpException
	 */
	public boolean argsValidation() throws OpException; 
	
	
//	/**
//	 * Compare an operation with another in means of issued time
//	 */
//	public int compareTo(Operation op);
	
}
