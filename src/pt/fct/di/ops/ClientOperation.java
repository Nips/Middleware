package pt.fct.di.ops;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import pt.fct.di.db.ConsistencyLevel;

public abstract class ClientOperation implements IClientOperation{

	/**
	 * Type of the operation - Update or Read?
	 */
	public int _type;
	
	/**
	 * Client's public id.
	 */
	public int _id;
	
	/**
	 * Operation sequence number.
	 */
	public int _opSeq;
	
	/**
	 * Column Family this operations respects to.
	 */
	public String _columnFamily;
	
	/**
	 * Row key this operation respects to.
	 */
	public String _rowKey;
	
	/**
	 * Version Vector of the Client.
	 */
	public long[] _versionVector;
	
	/**
	 * Consistency Level chosen by a client.
	 */
	public ConsistencyLevel _cl;
	
	/**
	 * Builds an empty operation
	 */
	public ClientOperation()
	{
	}
	
	/**
	 * Builds a new generic operation with the column family and row key to access. 
	 * Also contains the consistency level to preserve in the middleware.
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param cl
	 */
	public ClientOperation(int type, String columnFamily, String rowKey, ConsistencyLevel cl )
	{
		_type = type;
		_columnFamily = columnFamily;
		_rowKey = rowKey;
		_cl = cl;
		_id = -1;
		_opSeq = -1;
	}
	
	public ClientOperation(int type, int id, int opSeq, String columnFamily, String rowKey, ConsistencyLevel cl )
	{
		_type = type;
		_id = id;
		_opSeq = opSeq;
		_columnFamily = columnFamily;
		_rowKey = rowKey;
		_cl = cl;
	}
	
	public void destroyOp()
	{
		_columnFamily = null;
		_rowKey = null;
		_cl = null;
	}
	
	/************************************************/
	/**				  	Setters						*/
	/************************************************/
	
	public void setID(int id)
	{
		this._id = id;
	}
	
	public void setOpSeq(int seq)
	{
		this._opSeq = seq;
	}
	
	/**
	 * Sets a new column family for the operation
	 * @param family
	 */
	public void setColumnFamily(String family)
	{
		this._columnFamily = family;
	}
	
	/**
	 * Sets a new row key for the operation
	 * @param key
	 */
	public void setRowKey(String key)
	{
		this._rowKey = key;
	}

	/**
	 * Sets the version vector of the client
	 */
	public void setVersionVector(long[] vector)
	{
		this._versionVector = vector;
	}
	
	/**
	 * Sets a new consistency level for the operation
	 */
	public void setConsistency(ConsistencyLevel cl)
	{
		this._cl = cl;
	}
	
	/************************************************/
	/**				  	Getters						*/
	/************************************************/
	
	public int getType()
	{
		return this._type;
	}

	public int getID()
	{
		return this._id;
	}
	
	public int getOpSeq()
	{
		return this._opSeq;
	}
	
	/**
	 * Gets the column family of the operation
	 * @return
	 */
	public String getColumnFamily()
	{
		return this._columnFamily;
	}
	
	/**
	 * Gets the row key of the operation
	 * @return
	 */
	public String getRowKey(){
		return this._rowKey;
	}
	
	/**
	 * Gets the fields or the values depending on the operation associated
	 */
	public abstract FieldsOrValues getFieldsOrValues();

	/**
	 * Gets the version vector of the client
	 */
	public long[] getVersionVector()
	{
		return this._versionVector;
	}
	
	/**
	 * Gets the consistency level for this operation
	 */
	public ConsistencyLevel getConsistency()
	{
		return this._cl;
	}
	
	
	/********************************************/
	/**				Serialization				*/
	/********************************************/


	/**
	 * Serialize this operation
	 */
	public abstract byte[] serialize() throws IOException;

	/**
	 * Serialize this operation to an OutputStream
	 */
	public abstract void serialize(ObjectOutputStream ois) throws IOException;
	
	/**
	 * Serialize this operation and return it in a data buffer
	 */
	public abstract byte[] toByteArray() throws OpException;

	/**
	 * Deserialize this operation given a data buffer
	 */
	public abstract void deserialize(byte[] buffer) throws IOException, ClassNotFoundException;

	/**
	 * Deserialize this operation given an InputStream
	 */
	public abstract void deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException;

	/**
	 * Validate the correctness of the arguments
	 */
	public abstract boolean argsValidation() throws OpException;

	/**
	 * Validate this operation
	 */
	public boolean opValidation() throws OpException
	{
		if(_id < 0)
			throw new OpException("Required field \"id\" was not present in operation "+getType());
		if(_opSeq < 0)
			throw new OpException("Required field \"opSeq\" was not present in operation "+getType());
		if(_versionVector == null)
			throw new OpException("Required field timestamp was not present in operation "+getType());
		return true;
	}

	public int compareTo(Operation op)
	{
//		if(getTS() <  op.getTS()) return -1;
//		else if(getTS() > op.getTS()) return 1;
//		else return getID().compareTo(op.getID());
		return 0;
	}
}
