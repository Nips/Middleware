package pt.fct.di.ops.log;

//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;
import pt.fct.di.ops.FieldsOrValues;
import pt.fct.di.ops.OpException;
import pt.fct.di.util.Pair;

public class NoOp extends LogOperation {

	public static final int TYPE = 3;
	
	public NoOp(){ 
		super();
	}
	
	public NoOp(int id, long timestamp)
	{
		super(TYPE, id, timestamp);
	}
	
	@Override
	public void setFields(Set<String> fields) {
		// Not supported - a NoOp operation doesn't have a set of fields assign
	}
	
	@Override
	public String getColumnFamily() {
		// Not Supported - a NoOp operation doesn't have a column family assigned.
		return null;
	}

	@Override
	public String getRowKey() {
		// Not Supported - a NoOp operation doesn't have a row key assigned.  
		return null;
	}
	
	/**
	 * Get family column and key to access wrapped in a Pair object
	 */
	@Override
	public Pair<String, String> getFamilyAndKey()
	{
		//Not Supported
		return null;
	}
	
	@Override
	public Set<String> getFields() {
		// Not supported - a NoOp operation doesn't have a set of fields assign
		return null;
	}
	
	@Override
	public long[] getVersionVector() {
		long[] vv = {_timestamp};
		return vv;
	}

//	@Override
//	public byte[] serialize() throws IOException {
//		//create outputstreams
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		ObjectOutputStream dos = new ObjectOutputStream(baos);
//		
//		//serialize
//		dos.writeUTF(_type);
//		dos.writeInt(_id);
//		dos.writeLong(_timestamp);
//		baos.flush();
//		dos.flush();
//		byte[] result = baos.toByteArray();
//		
//		//close outputs
//		dos.close();
//		return result;
//	}
//
//	@Override
//	public void serialize(ObjectOutputStream oos) throws IOException {
//		//serialize
//		oos.writeUTF(_type);
//		oos.writeInt(_id);
//		oos.writeLong(_timestamp);
//
//		oos.flush();
//	}
//
//	@Override
//	public void deserialize(byte[] buffer) throws IOException,
//			ClassNotFoundException {
//		//create inputStreams
//		ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
//		ObjectInputStream ois = new ObjectInputStream(bais);
//		
//		//deserialize
//		deserialize(ois);
//		
//		ois.close();
//	}
//
//	@Override
//	public void deserialize(ObjectInputStream ois) throws IOException,
//			ClassNotFoundException {
//		//deserialize
//		this._id = ois.readInt();
//		this._timestamp = ois.readLong();
//	}
//
//	@Override
//	public byte[] toByteArray() throws OpException {
//		//validate operation
//		opValidation();
//		
//		//Create byte array
//		try{
//			return serialize();
//		}
//		catch(IOException e){
//			OpException eop = new OpException(e.getMessage());
//			eop.setStackTrace(e.getStackTrace());
//			throw eop;
//		}
//	}
//
//	@Override
//	public boolean argsValidation() throws OpException {
//		//Not Supported
//		return true;
//	}
	
	@Override
	public String toString() {
		return "NoOp [_id=" + _id + ", _timestamp="+_timestamp+"]";
	}

	@Override
	public boolean opValidation() throws OpException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void serialize(ObjectOutputStream ois) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void deserialize(ObjectInputStream ois) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FieldsOrValues getFieldsOrValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConsistencyLevel getConsistency() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateFields(Set<String> newFields) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ILogOperation convertToLog() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ILogOperation convertToLog(DB _remotedb) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unsetFieldsOrValues() {
		// TODO Auto-generated method stub
		
	}

}
