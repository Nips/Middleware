package pt.fct.di.ops.log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Set;

import pt.fct.di.ops.OpException;
import pt.fct.di.util.Pair;

public abstract class LogOperation implements ILogOperation{
	
	public int _type;
	public int _id;
	public int _seq;
	public long _timestamp;
	private boolean _isSetTimestamp = false;

	public LogOperation(){ }
	
	public LogOperation(int type, int id, int seq)
	{
		this._type = type;
		this._id = id;
		this._seq = seq;
		this._isSetTimestamp = false;
	}
	
	public LogOperation(int type, int id, int seq, long timestamp)
	{
		this._type = type;
		this._id = id;
		this._seq = seq;
		this._timestamp = timestamp;
		this._isSetTimestamp = true;
	}
	
	public int getOpSeq()
	{
		return _seq;
	}
	
	/**
	 * Sets a new set of fields for the operation
	 * @param fields
	 */
	public abstract void setFields(Set<String> fields);

	/**
	 * Sets a new timestamp for the operation
	 */
	public void setTS(long timestamp)
	{
		this._timestamp = timestamp;
		this._isSetTimestamp = true;
	}

	public int getType()
	{
		return _type;
	}

	public int getID() {
		return _id;
	}
	
	/**
	 * Get column family
	 */
	public abstract String getColumnFamily();

	/**
	 * Get row key
	 */
	public abstract String getRowKey();
	
	/**
	 * Get family column and key to access wrapped in a Pair object
	 */
	public abstract Pair<String, String> getFamilyAndKey();
	
	/**
	 * Gets the set of fields for the operation
	 * @param fields
	 */
	public abstract Set<String> getFields();

	/**
	 * Get timestamp
	 * @return
	 */
	public long getTS()
	{
		return _timestamp;
	}
	
	/**
	 * Is timestamp set for this operation?
	 * @return
	 */
	public boolean isSetTimestamp()
	{
		return this._isSetTimestamp;
	}
	
	@Override
	public long[] getVersionVector() {
		long[] vv = {_timestamp};
		return vv;
	}

	@Override
	public abstract void serialize(ObjectOutputStream ois) throws IOException;
	
//
//	@Override
//	public abstract void deserialize(byte[] buffer) throws IOException,
//			ClassNotFoundException;
//
//	@Override
//	public abstract void deserialize(ObjectInputStream ois) throws IOException,
//			ClassNotFoundException;
//
//	@Override
//	public abstract byte[] toByteArray() throws OpException;
//
//	@Override
//	public abstract boolean argsValidation() throws OpException;
//
//	/**
//	 * Validate the correctness of the arguments
//	 */
//	@Override
//	public boolean opValidation() throws OpException {
//		if(_id < 0)
//			throw new OpException("Required field \"id\" was not present in operation "+getType());
//		if(!isSetTimestamp())
//			throw new OpException("Required field timestamp was not present in operation "+getType());
//		return true;
//	}
	
	/**
	 * Validate the correctness of the arguments
	 */
	@Override
	public boolean opValidation() throws OpException {
		if(_id < 0)
			throw new OpException("Required field \"id\" was not present in operation "+getType());
		if(!isSetTimestamp())
			throw new OpException("Required field timestamp was not present in operation "+getType());
		return true;
	}

	@Override
	public int compareTo(ILogOperation otherOp)
	{
		long otherTs = otherOp.getTS();
//		System.out.println("Our op id: "+_id+" with ts: "+_timestamp+" , OtherOp id: "+otherOp.getID()+" with ts: "+otherOp.getTS());
		if(_timestamp <  otherTs) return -1;
		else if(_timestamp > otherTs) return 1;
		else
		{
//			System.out.println("Equal timestamp!!!");
			int otherId = otherOp.getID();
			if(_id < otherId) return -1;
			else if(_id > otherId) return 1;
			else
			{
				int otherOpSeq = otherOp.getOpSeq();
				//only if the two operations are equal in timestamp and id, the attribute seq is used to order them.
				//The rule is operation out of order with a greater seq must come before the early one seen in log (to have the same order
				//given in Structured Log).
				if(_seq >= otherOpSeq) return -1;
//				else if (_seq < otherOpSeq) return 1;
				return 1;
			}
		}
	}
	
	public int compare(ILogOperation otherOp)
	{
		long otherTs = otherOp.getTS();
//		System.out.println("Our op id: "+_id+" with ts: "+_timestamp+" , OtherOp id: "+otherOp.getID()+" with ts: "+otherOp.getTS());
		if(_timestamp <  otherTs) return -1;
		else if(_timestamp > otherTs) return 1;
		else
		{
//			System.out.println("Equal timestamp!!!");
			int otherId = otherOp.getID();
			if(_id < otherId){
//				System.out.println("Log op with id < otherId!!!");
				return -1;
			}
			else if(_id > otherId)
			{
//				System.out.println("Log op with id > otherId!!!");
				return 1;
			}
			else return 0;
		}	
	}
}
