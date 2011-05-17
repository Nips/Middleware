package pt.fct.di.ops;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;

public class Read extends ClientOperation {
	
	private final static int TYPE = 3;
	//private final boolean ASYNC = false;
	
	private FieldsOrValues _fields;
	
//	/**
//	 * Creates an empty Insert operation
//	 */
//	public Read()
//	{
//		super();
//		_fields = new FieldsOrValues();
//	}
	
//	/**
//	 * Builds a new Read operation with the column family and row key to access.
//	 * Also contains the consistency level to preserve by the middleware.
//	 * 
//	 * @param columnFamily
//	 * @param rowKey
//	 * @param cl
//	 */
//	public Read(String columnFamily, String rowKey, ConsistencyLevel cl)
//	{
//		this(columnFamily, rowKey, new HashSet<String>(), cl);
//	}
	
	/**
	 * Builds a new Read operation with the column family and row key to access and fields to read.
	 * Also contains the consistency level to preserve by the middleware.
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param fields
	 * @param cl
	 */
	public Read(String columnFamily, String rowKey, Set<String> fields, ConsistencyLevel cl)
	{
		super(TYPE, columnFamily, rowKey, cl);
		this._columnFamily = columnFamily;
		this._rowKey = rowKey;
		this._fields = new FieldsOrValues(fields);
		this._cl = cl;
	}
	
	public Read(int id, int opSeq, String columnFamily, String rowKey, Set<String> fields, ConsistencyLevel cl)
	{
		super(TYPE, id, opSeq, columnFamily, rowKey, cl);
		this._columnFamily = columnFamily;
		this._rowKey = rowKey;
		this._fields = new FieldsOrValues(fields);
		this._cl = cl;
	}
	
	/**
	 * Creates a new Insert operation given a data buffer
	 * 
	 * @param buffer
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Read(byte[] buffer) throws IOException, ClassNotFoundException
	{
		_type = TYPE;
		deserialize(buffer);
	}
	
	/**
	 * Creates a new Insert operation given an Inputstream
	 * 
	 * @param ois
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Read(ObjectInputStream ois) throws IOException, ClassNotFoundException
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
	
	public FieldsOrValues getFieldsOrValues()
	{
		return this._fields;
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
	
	public byte[] serialize() throws IOException {
		//create outputstreams
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		//serialize
		oos.writeInt(TYPE);
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
		oos.writeInt(TYPE);
		oos.writeInt(_id);
		oos.writeInt(_opSeq);
		oos.writeUTF(_columnFamily);
		oos.writeUTF(_rowKey);
		writeFields(oos);
		writeVersionVector(oos);
		oos.writeInt(_cl.getValue());
		
		oos.flush();
	}

	@Override
	public byte[] toByteArray() throws OpException {
		//validate operation
//		opValidation();
		
		//Create byte array
		try
		{
			return serialize();
		}
		catch(IOException e)
		{
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
		this._id = ois.readInt();
		this._opSeq = ois.readInt();
		this._columnFamily = ois.readUTF();		
		this._rowKey = ois.readUTF();
		readFields(ois);
		readVersionVector(ois);
		this._cl = ConsistencyLevel.findByValue(ois.readInt());
	}
	
	public boolean argsValidation() throws OpException {

		if(_columnFamily == null)
			throw new OpException("Required field \"column family\" was not present in operation Read");
		if(_rowKey == null)
			throw new OpException("Required field \"row key\" was not present in operation Read");
		if(_fields == null)
			throw new OpException("Required field \"fields\" was not present in operation Read. If you want to read all fields for some " +
					"family and row you can give an empty set to the constructor");
		if(_cl == null)
			throw new OpException("Required Consistency Level was not entered for operation Read");
		return true;
	}
	
	private String toStringFields()
	{
		String fields = "";
		//for(String nameField: _fields)
		Iterator<String> field = _fields.getFields().iterator();
		while(field.hasNext())
			fields += field.next()+", ";
		return fields;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Read [TYPE=" + TYPE + ", _id=" + _id + ", _columnFamily="
				+ _columnFamily + ", _rowKey=" + _rowKey + ", _fields: "
				+ toStringFields() + "_versionVector=" + _versionVector + "]";
	}

	@Override
	public void updateFields(Set<String> newFields) {
		setFields(newFields);	
	}
}
