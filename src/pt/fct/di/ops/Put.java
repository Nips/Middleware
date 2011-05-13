package pt.fct.di.ops;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.ops.log.PutLog;
import pt.fct.di.util.Pair;

public class Put extends ClientOperation implements LoggableOperation{

	private final static int TYPE = 1; //Update or Read?
	private FieldsOrValues _values;
	
	/**
	 * Creates an empty Insert operation
	 */
	public Put()
	{
		super();
		_values = new FieldsOrValues();
	}

	/**
	 * Builds a new Put operation with the column family to access and row key with respective values to insert.
	 * Also contains the consistency level to preserve by the middleware.
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param values
	 * @param cl
	 */
	public Put(String columnFamily, String rowKey, HashMap<String, String> values, ConsistencyLevel cl)
	{
		super(TYPE, columnFamily, rowKey, cl);
		this._values = new FieldsOrValues(values);
	}
	
	/**
	 * Builds a new Put operation with the column family to access and row key with respective values to insert.
	 * Also contains the consistency level to preserve by the middleware.
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param values
	 * @param cl
	 */
	public Put(int id, int opSeq, String columnFamily, String rowKey, HashMap<String, String> values, ConsistencyLevel cl)
	{
		super(TYPE, id, opSeq, columnFamily, rowKey, cl);
		this._values = new FieldsOrValues(values);
	}
	
	/**
	 * Creates a new Put operation given a data buffer
	 * 
	 * @param buffer
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Put(byte[] buffer) throws IOException, ClassNotFoundException
	{
		_type = TYPE;
		deserialize(buffer);
	}
	
	/**
	 * Creates a new Put operation given an Inputstream
	 * 
	 * @param ois
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Put(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{
		_type = TYPE;
		deserialize(ois);
	}
	
	public void destroyOp()
	{
		super.destroyOp();
		_values = null;
	}
	
	public void setValues(HashMap<String, String> values)
	{
		this._values.setValues(values);
	}
	
	public FieldsOrValues getFieldsOrValues()
	{
		return this._values;
	}
	
	/**
	 * Get family column and key to access wrapped in a Pair object
	 */
	public Pair<String, String> getFamilyAndKey()
	{
//		System.out.println(_columnFamily);
		return new Pair<String, String>(_columnFamily, _rowKey );
	}
	
	private void writeValues(ObjectOutputStream oos) throws IOException
	{
		oos.writeInt(_values.getSize());
		for(Map.Entry<String, String> entry : _values.getValues().entrySet())
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
		Map<String,String> map = new HashMap<String,String>(mapSize);
		for(int mapElems=0; mapElems<mapSize; mapElems++)
			map.put(ois.readUTF(), ois.readUTF());
		_values = new FieldsOrValues(map);
	}
	
	private void readVersionVector(ObjectInputStream ois) throws IOException
	{
		int vectorSize = ois.readInt();
		_versionVector = new long[vectorSize];
		for(int pos=0; pos<vectorSize; pos++)
			_versionVector[pos] = ois.readLong();
	}
	
	/**
	 * Serialize this operation
	 */
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
		writeValues(oos);
		writeVersionVector(oos);
		oos.writeInt(_cl.getValue());
		baos.flush();
		oos.flush();
		byte[] result = baos.toByteArray();
		
		//close outputs
		oos.close();
		return result;
	}
	
	/**
	 * Serialize this operation to an OutputStream
	 */
	public void serialize(ObjectOutputStream oos) throws IOException
	{	
		//serialize
		oos.writeInt(_type);
		oos.writeInt(_id);
		oos.writeInt(_opSeq);
		oos.writeUTF(_columnFamily);
		oos.writeUTF(_rowKey);
		writeValues(oos);
		writeVersionVector(oos);
		oos.writeInt(_cl.getValue());

		oos.flush();
	}
	
	/**
	 * Serialize this operation and return it in a data buffer
	 */
	public byte[] toByteArray() throws OpException
	{
		//validate operation
		//opValidation();
		
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
	
	/**
	 * Deserialize this operation given a data buffer
	 */
	public void deserialize(byte[] buffer) throws IOException, ClassNotFoundException
	{	
		//create inputStreams
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
		ObjectInputStream ois = new ObjectInputStream(bais);
		
		//deserialize
		deserialize(ois);
		
		ois.close();
	}

	/**
	 * Deserialize this operation given an InputStream
	 */
	public void deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException
	{	
		//deserialize
		this._id = ois.readInt();
		this._opSeq = ois.readInt();
		this._columnFamily = ois.readUTF();
		this._rowKey = ois.readUTF();
		readValues(ois);
		readVersionVector(ois);
		this._cl = ConsistencyLevel.findByValue(ois.readInt());
	}
	
	/**
	 * Validate the correctness of the arguments
	 */
	public boolean argsValidation() throws OpException
	{
		if(_columnFamily == null)
			throw new OpException("Required field \"column family\" was not present in operation Insert");
		if(_rowKey == null)
			throw new OpException("Required field \"row key\" was not present in operation Insert");
		if(!_values.isSetValues())
			throw new OpException("Required field \"values\" was not present in operation Insert");
		if(_cl == null)
			throw new OpException("Required Consistency Level was not entered for operation Insert");
		return true;
	}
	
	private String toStringValues()
	{
		String values = " ";
		for(Map.Entry<String, String> value: _values.getValues().entrySet())
			values += value.getKey() + "=" + value.getValue() + ", ";
		return values;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Put [TYPE=" + TYPE + ", _id=" + _id + ", _columnFamily="
				+ _columnFamily + ", _rowKey=" + _rowKey + ", _values:"
				+ toStringValues() + ", _versionVector=" + _versionVector + "]";
	}

	@Override
	public ILogOperation convertToLog() {
		return new PutLog(_id, _columnFamily, _rowKey, _values.getValues(), _versionVector[_id]+1); //Change this
	}
	
	@Override
	public ILogOperation convertToLog(DB remotedb) {
		return new PutLog(_id, _columnFamily, _rowKey, _values.getValues().keySet(), _versionVector[_id]+1, remotedb); //Change this
	}

	/** After transforming an operation the final set of fields that not conflict with new operation log entries is 
	 * obtained. With this auxiliar method the values/fields of the original operation are substituted by fields 
	 * in argument to preserve transformation.
	 **/  
	@Override
	public void updateFields(Set<String> maintainFieldsSet) {
		Map<String, String> currentValues = _values.getValues();
		HashMap<String, String> finalValues = new HashMap<String,String>();
		for(String field: maintainFieldsSet)
			finalValues.put(field, currentValues.get(field));
		setValues(finalValues);
		return;
	}

	@Override
	public void unsetFieldsOrValues() {
		_values.unsetValues();
		_values = null;
	}
}
