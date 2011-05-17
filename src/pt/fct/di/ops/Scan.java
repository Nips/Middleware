package pt.fct.di.ops;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;

public class Scan extends ClientOperation {

	
	private final static int TYPE = 4;
	private final int RECORDS_TO_READ = 10; 
	
	private int _recordCount = RECORDS_TO_READ;
	private FieldsOrValues _fields;
	
//	/**
//	 * Creates an empty Scan operation
//	 */
//	public Scan()
//	{
//		super();
//		_fields = new FieldsOrValues();
//	}
	
	/**
	 * Builds a new Scan operation with the column family and start row key to access. Reads many following keys 
	 * as indicated in recordCount.
	 * Also contains the consistency level to preserve in the middleware. 
	 * 
	 * @param columnFamily
	 * @param startRowKey
	 * @param recordCount
	 * @param cl
	 */
	public Scan(String columnFamily, String startRowKey, int recordCount, ConsistencyLevel cl)
	{
		this(columnFamily, startRowKey, recordCount, new HashSet<String>(), cl);
	}
	
	/**
	 * Builds a new Scan operation with the column family and start row key to access. Reads many following keys 
	 * and respective fields as indicated in recordCount.
	 * Also contains the consistency level to preserve in the middleware. 
	 * 
	 * @param columnFamily
	 * @param startRowKey
	 * @param recordCount
	 * @param cl
	 */
	public Scan(String columnFamily, String startRowKey, int recordCount, Set<String> fields,
			ConsistencyLevel cl)
	{
		super(TYPE, columnFamily, startRowKey, cl);
		this._columnFamily = columnFamily;
		if(recordCount > 0) this._recordCount = recordCount;
		this._fields = new FieldsOrValues(fields);
		this._cl = cl;
	}
	
	public Scan(int id, int opSeq, String columnFamily, String startRowKey, int recordCount, Set<String> fields,
			ConsistencyLevel cl)
	{
		super(TYPE, id, opSeq, columnFamily, startRowKey, cl);
		this._columnFamily = columnFamily;
		if(recordCount > 0) this._recordCount = recordCount;
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
	public Scan(byte[] buffer) throws IOException, ClassNotFoundException
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
	public Scan(ObjectInputStream ois) throws IOException, ClassNotFoundException
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
	
	public String getStartRowKey()
	{
		return this._rowKey;
	}
	
	public int getRecordCount() 
	{
		return this._recordCount;
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
		oos.writeInt(_recordCount);
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
		oos.writeInt(_recordCount);
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
		this._recordCount = ois.readInt();
		readFields(ois);
		readVersionVector(ois);
		this._cl = ConsistencyLevel.findByValue(ois.readInt());
	}
	
	public boolean argsValidation() throws OpException 
	{
		if(_columnFamily == null)
			throw new OpException("Required field \"column family\" was not present in operation Scan");
		if(_rowKey == null)
			throw new OpException("Required field \"row key\" was not present in operation Scan");
		if(_fields == null)
			throw new OpException("Required field \"fields\" was not present in operation Scan. If you want to read all fields for a " +
					"family and row you can give an empty set to the constructor");
		if(_cl == null)
			throw new OpException("Required Consistency Level was not entered for operation Scan");
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
		return "Scan [TYPE=" + TYPE + ", _id=" + _id + ", _columnFamily="
				+ _columnFamily + ", _startRowKey=" + _rowKey
				+ ", _recordCount=" + _recordCount + ", _fields:"
				+ toStringFields() +  "_versionVector=" + _versionVector + "]";
	}

	@Override
	public void updateFields(Set<String> newFields) {
		setFields(newFields);
	}
}
