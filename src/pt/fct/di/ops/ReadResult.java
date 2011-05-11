package pt.fct.di.ops;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class ReadResult implements IResult{
	
	private final int RESULT_TYPE = 6;
	
//	private int _type; //Changed from String to Integer because we know all types from starter and can attribute a number. This saves space.
	private int _opSeq; //Changed to only store the sequence number of the operation that generated this result. It is needed in the client side.
	private int _code;
	private Map<String,String> _values; //in the case code < 0 an error message will be put here.
	private String _msg;
	private long[] _versionVector; //Changed from timestamp to store the version vector.
	
	public ReadResult(int code)
	{
		this._opSeq = 0;
		this._code = code;
		this._values = null;
		this._msg = "";
		this._versionVector = null;
	}
	
	public ReadResult(int code, Map<String,String> values)
	{
		this._opSeq = 0;
		this._code = code;
		this._values = values;
		this._versionVector = null;
	}
	
	public ReadResult(int code, String msg)
	{
		this._opSeq = 0;
		this._code = code;
		this._values = null;
		this._msg = msg;
		this._versionVector = null;
	}
	
	public ReadResult(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{
		deserialize(ois);
	}
	
	public void destroyResult()
	{
		_values = null;
		_msg = null;
		_versionVector = null;
	}
	
	public void setOpSeq(int seq)
	{
		this._opSeq = seq;
	}
	
	public void setCode(int code)
	{ 
		this._code = code;
	}
	
	/*public void setColumnFamily(String columnFamily) {
		this._columnFamily = columnFamily;
	}*/
	
	/*public void setPath(String[] path) {
		this._path = path;
	}*/


	public void setMsg(String msg) {
		if(_code < 0) this._msg = msg;
	}

	public void setValues(Map<String,String> values)
	{
		if(_code >= 0) this._values = values;
	}
	
	public void setVersionVector(long[] vector)
	{
		this._versionVector = vector;
	}
	
	public int getOpSeq()
	{
		return this._opSeq;
	}
	
	public int getCode()
	{
		return this._code;
	}
	
	public Map<String,String> getValues()
	{
		return this._values;
	}
	
	public String getMsg() 
	{
		return _msg;
	}
	
	public long[] getVersionVector()
	{
		return this._versionVector;
	}
	
	private void writeValues(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_values.size());
		for(Map.Entry<String, String> entry : _values.entrySet())
		{
			oos.writeUTF(entry.getKey());
			oos.writeUTF(entry.getValue());
		}
	}
	
	private void writeVersionVector(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_versionVector.length);
		for(int pos=0; pos<_versionVector.length; pos++)
			oos.writeLong(_versionVector[pos]);
	}
	
	private void readValues(ObjectInputStream ois) throws IOException
	{
		int mapSize = ois.readInt();
		_values = new HashMap<String,String>(mapSize);
		for(int mapElems=0; mapElems<mapSize; mapElems++)
			_values.put(ois.readUTF(), ois.readUTF());
	}
	
	private void readVersionVector(ObjectInputStream ois) throws IOException
	{
		int vectorSize = ois.readInt();
		_versionVector = new long[vectorSize];
		for(int pos=0; pos<vectorSize; pos++)
			_versionVector[pos] = ois.readLong();
	}
	
	public byte[] serialize() throws IOException
	{
		//create outputstreams
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		//serialize
		oos.writeInt(RESULT_TYPE);
		oos.writeInt(_opSeq);
		oos.writeInt(_code);
		if(_code < 0) oos.writeUTF(_msg);
		else writeValues(oos);
		writeVersionVector(oos);
		baos.flush();
		oos.flush();
		byte[] result = baos.toByteArray();
		
		//close outputs
		oos.close();
		return result;
	}
	
	public void serialize(ObjectOutputStream oos) throws IOException, OpException
	{		
		//serialize
		oos.writeInt(RESULT_TYPE);
		oos.writeInt(_opSeq);
		oos.writeInt(_code);
		if(_code < 0) oos.writeUTF(_msg);
		else writeValues(oos);
		writeVersionVector(oos);
		
		oos.flush();
	}
	
	public byte[] toByteArray() throws OpException
	{
		//validate operation
		//opValidation();
		
		//Create byte array
		try{
			return serialize();
		}
		catch(IOException e){
			System.out.println(e.getMessage());;
			OpException eop = new OpException(e.getMessage());
			eop.setStackTrace(e.getStackTrace());
			throw eop;
		}
	}
	
	
	public void deserialize(byte[] buffer) throws IOException, ClassNotFoundException
	{	
		//create inputStreams
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
		ObjectInputStream ois = new ObjectInputStream(bais);
		
		//deserialize
		deserialize(ois);
		
		//close outputs
		ois.close();
	}
	
	public void deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{	
		//deserialize
		this._opSeq = ois.readInt();
		this._code = ois.readInt();
		if(_code < 0) _msg = ois.readUTF();
		else
		{
			readValues(ois);
			_msg = "";
		}
		readVersionVector(ois);
	}
	
	public boolean opValidation() throws OpException
	{
		if(this._opSeq < 0)
			throw new OpException("Field \"Operation Sequence\" is missing in operation Result");
		if(_code > 0 && this._values == null)
			throw new OpException("Field \"values\" must be not null in operation Result. It has to contain " +
					"one message or a result for some operation");
//		if(this._versionVector == null && (_type == 1|| _type == 2))
//			throw new OpException("Field \"Version Vector\" is not set in operation Result");
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Result [ _opSeq=" + _opSeq + ", _code=" + _code
				+ ", _versionVector=" + _versionVector + "]";
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	/*@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Result))
			return false;
		Result other = (Result) obj;
		if (_id == null) {
			if (other._id != null)
				return false;
		} else if (!_id.equals(other._id))
			return false;
		if (_type == null) {
			if (other._type != null)
				return false;
		} else if (!_type.equals(other._type))
			return false;
		return true;
	}*/
}