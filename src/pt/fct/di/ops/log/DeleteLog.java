package pt.fct.di.ops.log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;
import pt.fct.di.ops.FieldsOrValues;
import pt.fct.di.util.Pair;
public class DeleteLog extends LogOperation{

	public static final int TYPE = 2;

	public String _columnFamily;
	public String _rowKey;
	public FieldsOrValues _fields;
	
	public DeleteLog(){ 
		super();
	}
	
//	public DeleteLog(int id, String columnFamily, String rowKey, 
//			Set<String> fields)
//	{
//		super(TYPE, id);
//		this._columnFamily = columnFamily;
//		this._rowKey = rowKey;
//		this._fields = fields;
//	}
//	
//	public DeleteLog(int id, String columnFamily, String rowKey, 
//			Set<String> fields, long timestamp)
//	{
//		super(TYPE, id, timestamp);
//		this._columnFamily = columnFamily;
//		this._rowKey = rowKey;
//		this._fields = fields;
//	}
	
	public DeleteLog(int id, Set<String> fields)
	{
		super(TYPE, id);
		this._fields = new FieldsOrValues(fields);
	}
	
	public DeleteLog(int id, String columnFamily, String rowKey, Set<String> fields, long timestamp)
	{
		super(TYPE, id, timestamp);
		this._columnFamily = columnFamily;
		this._rowKey = rowKey;
		this._fields = new FieldsOrValues(fields);
	}
	
	public DeleteLog(ObjectInputStream ois) throws IOException
	{
		_type = TYPE;
		this.deserialize(ois);
	}
	
//	/**
//	 * Sets a new column family for the operation
//	 * @param family
//	 */
//	public void setColumnFamily(String family)
//	{
//		this._columnFamily = family;
//	}
//	
//	/**
//	 * Sets a new row key for the operation
//	 * @param key
//	 */
//	public void setRowKey(String key)
//	{
//		this._rowKey = key;
//	}
	
	/**
	 * Sets new set of fields for the operation
	 */
	public void setFields(Set<String> fields) {
		_fields.setFields(fields);
		_fields.unsetValues();
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
	 * Get family column and key to access wrapped in a Pair object
	 */
	public Pair<String, String> getFamilyAndKey()
	{
		return new Pair<String, String>(_columnFamily, _rowKey );
	}
	
	/**
	 * Get the fields associated to the operation
	 */
	public Set<String> getFields() {
		return _fields.getFields();
	}
	
	private void writeFields(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_fields.getSize());
		for(String entry : _fields.getFields())
			oos.writeUTF(entry);
	}
	
	private void readFields(ObjectInputStream ois) throws IOException
	{
		int size = ois.readInt();
		HashSet<String> fields = new HashSet<String>(size);
		for(int nElems = 0; nElems < size; nElems++)
			fields.add(ois.readUTF());
		_fields = new FieldsOrValues(fields);
	}

	/**
	 * Serialize this operation to an OutputStream
	 */
	@Override
	public void serialize(ObjectOutputStream oos) throws IOException {
		//serialize
		oos.writeInt(_type);
		oos.writeInt(_id);
		oos.writeUTF(_columnFamily);
		oos.writeUTF(_rowKey);
		writeFields(oos);
		oos.writeLong(_timestamp);

		oos.flush();
	}
	
	@Override
	public void deserialize(ObjectInputStream ois) throws IOException {
		_id = ois.readInt();
		_columnFamily = ois.readUTF();
		_rowKey = ois.readUTF();
		readFields(ois);
		_timestamp = ois.readLong();
	}
//
//	/**
//	 * Deserialize this operation given a data buffer
//	 */
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
//	/**
//	 * Deserialize this operation given an InputStream
//	 */
//	@Override
//	public void deserialize(ObjectInputStream ois) throws IOException,
//			ClassNotFoundException {
//		//deserialize
//		this._id = ois.readInt();
//		this._columnFamily = ois.readUTF();
//		this._rowKey = ois.readUTF();
//		this._timestamp = ois.readLong();
//	}
//
//	/**
//	 * Serialize this operation and return it in a data buffer
//	 */
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
//	/**
//	 * Deserialize this operation given an InputStream
//	 */
//	@Override
//	public boolean argsValidation() throws OpException {
//		if(_columnFamily == null)
//			throw new OpException("Required field \"column family\" was not present in operation log "+getType());
//		if(_rowKey == null)
//			throw new OpException("Required field \"row key\" was not present in operation log "+getType());
//		if(_fields == null)
//			throw new OpException("Required field \"fields\" was not present in operation log "+getType());
//		return true;
//	}
//
	
	private String readFields()
	{
		String result = " ( ";
		Iterator<String> it = _fields.getFields().iterator();
		while(it.hasNext())
		{
			result += it.next()+" ";
		}
		
		//System.out.println(result);
		return result+" ) ";
	}
	
	@Override
	public String toString() {
		return "Delete [_id=" + _id + ", _columnFamily="+ _columnFamily +", _rowKey= "+ _rowKey + ", _timestamp="+_timestamp+", _fields=" + readFields() + "]";
	}

	@Override
	public FieldsOrValues getFieldsOrValues() {
		return _fields;
	}

	@Override
	public ConsistencyLevel getConsistency() {
		return ConsistencyLevel.ONE;
	}

	@Override
	public void updateFields(Set<String> newFields) {
		//Not supported	
	}

	@Override
	public ILogOperation convertToLog() {
		return this;
	}

	@Override
	public ILogOperation convertToLog(DB _remotedb) {
		return this;
	}

	@Override
	public void unsetFieldsOrValues() {
		//Not Supported 
	}

//	@Override
//	public int compareTo(ILogOperation arg0) {
//		long otherTimestamp = arg0.getTS();
//		if(_timestamp < otherTimestamp) return -1;
//		else if(_timestamp > otherTimestamp) return 1;
//		else
//		{
//			int otherId = arg0.getID();
//			if(_id < otherId) return -1;
//			else if(_id > otherId) return 1;
//			else return 0;
//		}
//	}
}
