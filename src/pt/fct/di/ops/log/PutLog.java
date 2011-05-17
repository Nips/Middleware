package pt.fct.di.ops.log;

import java.io.IOException;
import java.io.ObjectInputStream;
//import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
//import java.util.HashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;
import pt.fct.di.ops.FieldsOrValues;
import pt.fct.di.util.Pair;

public class PutLog extends LogOperation{

	public final static int TYPE = 1;
	
	public String _columnFamily;
	public String _rowKey;
	private Set<String> _fields;
	private Values _values;
	
	/**
	 * This values are set only for synchronization propose.
	 */
	private FieldsOrValues _syncValues;

	
	public PutLog(){ 
		super();
	}
	
//	public PutLog(int id, String columnFamily, String rowKey,
//			Set<String> fields)
//	{
//		super(TYPE,id);
//		this._columnFamily = columnFamily;
//		this._rowKey = rowKey;
//		this._fields = fields;
//	}
//	
//	public PutLog(int id, String columnFamily, String rowKey, 
//			Set<String> fields, long timestamp)
//	{
//		super(TYPE, id, timestamp);
//		this._columnFamily = columnFamily;
//		this._rowKey = rowKey;
//		this._fields = fields;
//	}
	
	public PutLog(int id, Set<String> fields)
	{
		super(TYPE,id);
		this._fields = fields;
	}
	
	public PutLog(int id, String columnFamily, String key, Map<String,String> values, long timestamp)
	{
		super(TYPE, id, timestamp);
		this._columnFamily = columnFamily;
		this._rowKey = key;
		this._fields = values.keySet();
		setValues(values);
	}
	
	public PutLog(int id, String columnFamily, String key, Set<String> fields, long timestamp, DB remotedatabase)
	{
		super(TYPE, id, timestamp);
		this._columnFamily = columnFamily;
		this._rowKey = key;
		this._fields = fields;
		setValues(remotedatabase);
	}
	
	public PutLog(ObjectInputStream ois) throws IOException
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
		_fields = fields;
	}
	
	public void setValues(Map<String,String> localValues)
	{
		_values = new Values(localValues);
	}
	
	public void setValues(DB database)
	{
		_values = new Values(database);
	}

	public int getType() {
		return _type;
	}
	
	/**
	 * Get the column family of the operation
	 * @return
	 */
	public String getColumnFamily()
	{
		return this._columnFamily;
	}
	
	/**
	 * Get the row key of the operation
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
		return _fields;
	}
	
	public Map<String,String> getValues()
	{
		return _values.getValues(_columnFamily, _rowKey, _fields);
	}
	
	private void writeValues(ObjectOutputStream oos) throws IOException
	{
		Map<String,String> values = _values.getValues(_columnFamily, _rowKey, _fields);
		oos.writeInt(values.size());
		for(Map.Entry<String, String> entry : values.entrySet())
		{
			oos.writeUTF(entry.getKey());
			oos.writeUTF(entry.getValue());
		}
	}
	
	private void readValues(ObjectInputStream ois) throws IOException
	{
		int size = ois.readInt();
		Map<String,String> values = new HashMap<String,String>(size);
		_fields = new HashSet<String>(size);
		for(int nElems = 0; nElems < size; nElems++)
		{
			String field = ois.readUTF();
			values.put(field, ois.readUTF());
			_fields.add(field);
		}
		_syncValues = new FieldsOrValues(values);
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
		writeValues(oos);
		oos.writeLong(_timestamp);
		oos.flush();
	}

	/**
	 * Deserialize this operation given an InputStream
	 */
	@Override
	public void deserialize(ObjectInputStream ois) throws IOException{
		//deserialize
		this._id = ois.readInt();
		this._columnFamily = ois.readUTF();
		this._rowKey = ois.readUTF();
		readValues(ois);
		this._timestamp = ois.readLong();
	}

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
	
	private String readFields()
	{
		String result = " ( ";
		Iterator<String> it = _fields.iterator();
		while(it.hasNext())
		{
			result += it.next()+" ";
		}
		
		//System.out.println(result);
		return result+" ) ";
	}
	
	private String readValues()
	{
		Map<String,String> values = new HashMap<String,String>();
		if(_syncValues == null) values = _values.getValues(_columnFamily, _rowKey, _fields);
		else values = _syncValues.getValues();
		
		String result = " ( ";
		for(Map.Entry<String,String> entry : values.entrySet())
		{
			result += entry.getKey()+" "+entry.getValue()+", ";
		}
		
		//System.out.println(result);
		return result+" ) ";
	}
	
	@Override
	public String toString() {
		return "Put [_id=" + _id + ", _columnFamily="+ _columnFamily +", _rowKey= "+ _rowKey + ", _timestamp="+_timestamp+", " +
				"_fields=" + readFields() + "_values=" + readValues() + "]";
	}

	@Override
	public FieldsOrValues getFieldsOrValues() {
		return _syncValues;
	}

	@Override
	public ConsistencyLevel getConsistency() {
		return ConsistencyLevel.ONE;
	}

	@Override
	public void updateFields(Set<String> newFields) {
		Map<String, String> currentValues = _syncValues.getValues();
		HashMap<String, String> finalValues = new HashMap<String,String>(newFields.size());
		for(String field: newFields)
			finalValues.put(field, currentValues.get(field));
		_syncValues.setValues(finalValues);
		_values.setLocalValues(finalValues);
		return;
	}

	@Override
	public ILogOperation convertToLog() {
		_values = new Values(_syncValues.getValues());
		return this;
	}

	@Override
	public ILogOperation convertToLog(DB _remotedb) {
		_values = new Values(_remotedb);
		return this;
	}

	@Override
	public void unsetFieldsOrValues() {
		_syncValues.unsetValues();
		_syncValues = null;
	}
}
