package pt.fct.di.ops;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class UpdateResult implements IResult{
	
	private final int RESULT_TYPE = 5;
	
	private int _opSeq; //Changed to only store the sequence number of the operation that generated this result. It is needed in the client side.
	private int _code;
	private String _msg; //in the case code < 0 an error message will be put here.
	private long[] _versionVector; //Changed from timestamp to store the version vector.
	
	public UpdateResult(int code)
	{
		this._opSeq = -1;
		this._code = code;
		this._msg = "";
		this._versionVector = null;
	}
	
	public UpdateResult(int code, String msg)
	{
		this._opSeq = -1;
		this._code = code;
		this._msg = msg;
		this._versionVector = null;
	}
	
	public UpdateResult(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{
		deserialize(ois);
	}
	
	public void destroyResult()
	{
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
	
	public Object getMsg()
	{
		return this._msg;
	}
	
	public long[] getVersionVector()
	{
		return this._versionVector;
	}
	
	private void writeVersionVector(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_versionVector.length);
		for(int pos=0; pos<_versionVector.length; pos++)
			oos.writeLong(_versionVector[pos]);
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
//		opValidation();
		
		//serialize
		oos.writeInt(RESULT_TYPE);
		oos.writeInt(_opSeq);
		oos.writeInt(_code);
		if(_code < 0) oos.writeUTF(_msg);
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
		if(_code < 0) this._msg = ois.readUTF();
		else this._msg = "";
		readVersionVector(ois);
	}
	
	public boolean opValidation() throws OpException
	{
		if(this._opSeq < 0)
			throw new OpException("Field \"Operation Sequence\" is missing in operation Result");
		if(this._versionVector == null)
			throw new OpException("Field \"versionVector\" must be not null in operation Result.");
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Result [ _opSeq=" + _opSeq + ", _code=" + _code
				+ ", _msg="+ _msg+ ", _versionVector=" + _versionVector + "]";
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