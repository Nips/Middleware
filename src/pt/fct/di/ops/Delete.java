package pt.fct.di.ops;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;
import pt.fct.di.ops.log.DeleteLog;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.util.Pair;

public class Delete extends ClientOperation implements LoggableOperation{

	private final static int TYPE = 2; //Update or Read?
	
	private FieldsOrValues _fields;
	
//	/**
//	 * Creates an empty Delete operation
//	 */
//	public Delete()
//	{
//		super();
//		_fields = new FieldsOrValues();
//	}
	
//	/**
//	 * Builds a new Delete operation with the column family to access and row key to delete.
//	 * Also contains the consistency level to preserve by the middleware.
//	 * 
//	 * @param columnFamily
//	 * @param rowKey
//	 * @param cl
//	 */
//	public Delete(String columnFamily, String rowKey, ConsistencyLevel cl)
//	{
//		this(columnFamily, rowKey, new HashSet<String>(), cl);
//	}
	
	/**
	 * Builds a new Delete operation with the column family and row key to access and fields within to delete.
	 * Also contains the consistency level to preserve by the middleware.
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param fields
	 * @param cl
	 */
	public Delete(String columnFamily, String rowKey, Set<String> fields, ConsistencyLevel cl)
	{
		super(TYPE, columnFamily, rowKey, cl);
		this._fields = new FieldsOrValues(fields);
	}
	
	public Delete(int id, int opSeq, String columnFamily, String rowKey, Set<String> fields, ConsistencyLevel cl)
	{
		super(TYPE, id, opSeq, columnFamily, rowKey, cl);
		this._fields = new FieldsOrValues(fields);
	}
	
	/**
	 * Creates a new Delete operation given a data buffer
	 * 
	 * @param buffer
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Delete(byte[] buffer) throws IOException, ClassNotFoundException
	{
		_type = TYPE;
		deserialize(buffer);
	}
	
	/**
	 * Creates a new Delete operation given an Inputstream
	 * 
	 * @param ois
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Delete(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{
		_type = TYPE;
		deserialize(ois);
	}
	
	public void destroyOp()
	{
		super.destroyOp();
		_fields = null;
	}
	
	public void setFields(Set<String> fields)
	{
		this._fields.setFields(fields);
	}
	
	public int getType()
	{
		return TYPE;
	}
	
	public FieldsOrValues getFieldsOrValues()
	{
		return this._fields;
	}
	
	/**
	 * Get family column and key to access wrapped in a Pair object
	 */
	public Pair<String, String> getFamilyAndKey()
	{
		return new Pair<String, String>(_columnFamily, _rowKey );
	}
	
	private void writeFields(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_fields.getSize());
		for(String entry : _fields.getFields())
			oos.writeUTF(entry);
	}
	
	private void writeVersionVector(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_versionVector.length);
		for(int pos=0; pos<_versionVector.length; pos++)
			oos.writeLong(_versionVector[pos]);
	}
	
	private void readFields(ObjectInputStream ois) throws IOException
	{
		int setSize = ois.readInt();
		Set<String> set = new HashSet<String>(setSize);
		for(int setElems=0; setElems<setSize; setElems++)
			set.add(ois.readUTF());
		_fields = new FieldsOrValues(set);
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
		oos.writeInt(_type);
		oos.writeInt(_id);
		oos.writeInt(_opSeq);
		oos.writeUTF(_columnFamily);
		oos.writeUTF(_rowKey);
		writeFields(oos);
		writeVersionVector(oos);
		oos.writeInt(_cl.getValue());
		baos.flush();
		oos.flush();
		byte[] result = baos.toByteArray();
		
		//close outputs
		oos.close();
		return result;
	}
	
	public void serialize(ObjectOutputStream oos) throws IOException
	{		
		//serialize
		oos.writeInt(_type);
		oos.writeInt(_id);
		oos.writeInt(_opSeq);
		oos.writeUTF(_columnFamily);
		oos.writeUTF(_rowKey);
		writeFields(oos);
		writeVersionVector(oos);
		oos.writeInt(_cl.getValue());
		
		oos.flush();
	}
	
	public byte[] toByteArray() throws OpException
	{
		//validate operation
//		opValidation();
		
		//Create byte array
		try{
			return serialize();
		}
		catch(IOException e){
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
		
		ois.close();
	}
	
	public void deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{	
		//deserialize
		this._id = ois.readInt();
		this._opSeq = ois.readInt();
		this._columnFamily = ois.readUTF();
		this._rowKey = ois.readUTF();
		readFields(ois);
		readVersionVector(ois);
		this._cl = ConsistencyLevel.findByValue(ois.readInt());
	}
	
	public boolean argsValidation() throws OpException
	{

		if(_columnFamily == null)
			throw new OpException("Required field \"column family\" was not present in operation Remove.");
		if(_rowKey == null)
			throw new OpException("Required field \"row key\" was not present in operation Remove.");
		if(!_fields.isSetFields())
			throw new OpException("Required field \"fields\" was not present in operation Remove.");
		if(_cl == null)
			throw new OpException("Required Consistency Level was not entered for operation Remove.");
		return true;
	}
	
	private String toStringFields()
	{
		String fields = " ";
		for(String nameField: _fields.getFields())
			fields += nameField+", ";
		return fields;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Delete [TYPE=" + TYPE + ", _id=" + _id + ", _columnFamily="
				+ _columnFamily + ", _rowKey=" + _rowKey + ", _fields:"
				+ toStringFields() + "_versionVector=" + _versionVector + "]";
	}

	@Override
	public ILogOperation convertToLog() {
		return new DeleteLog(_id, _columnFamily, _rowKey, _fields.getFields(), _versionVector[_id]);
	}
	
	@Override
	public ILogOperation convertToLog(DB remotedb) {
		return new DeleteLog(_id, _columnFamily, _rowKey, _fields.getFields(), _versionVector[_id]);
	}

	@Override
	public void updateFields(Set<String> maintainFieldsSet) {
		setFields(maintainFieldsSet);
	}

	@Override
	public void unsetFieldsOrValues() {
		_fields.unsetFields();
		_fields = null;
	}
}
