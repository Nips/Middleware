package pt.fct.di.serverProxy.service;

//TODO: Implement client registry to fetch a DBInstance


import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import pt.fct.di.db.DB;
import pt.fct.di.db.DBException;
import pt.fct.di.db.DBFactory;

import pt.fct.di.ops.IResult;
import pt.fct.di.ops.LoggableOperation;
import pt.fct.di.ops.IClientOperation;
import pt.fct.di.ops.Scan;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.ops.log.PutLog;

import pt.fct.di.serverProxy.service.ServerService;
import pt.fct.di.serverProxy.service.log.ClientLog;
import pt.fct.di.serverProxy.service.log.SequencialLog;
import pt.fct.di.serverProxy.service.log.StructuredLog;
import pt.fct.di.serverProxy.service.time.LogicalTimer;
import pt.fct.di.serverProxy.service.time.Timer;
import pt.fct.di.serverProxy.sync.SyncException;
import pt.fct.di.util.Pair;

/**
 * ServerService implements the service on the server proxy.
 * 
 * Here is maintained and controlled the objects inserted by the clients.
 * We ensure that a new object is only inserted if it not exists already and
 * read or deleted if it was previously inserted. An update is always successful
 * because if an object does not exist an insertion is made.
 * 
 * @author andre_goncalves@di
 *
 */

public class ServerService {
//
//	private ThreadPool _pool = null;
//	private MessageBuffer _buffer = null;
//	private HReceiveMessages _handler = null;
//
	private SequencialLog _sequencialLog;
	private ClientLog _clientLog;
	private StructuredLog _structuredLog;
	private Timer _timer;
//	private boolean _debug = false;
	
	/**
	 * Database abstraction layer
	 */
	private DB _db;
	
	/**
	 * Property for getting remote values in sync mode
	 */
	private boolean _isRemoteValuesSet; 
	
	/**
	 * Properties for configuring this communication channel.
	 */
	private Properties _p=new Properties();

	/**
	 * Set the properties for this communication channel.
	 */
	public void setProperties(Properties p)
	{
		_p=p;
	}

	/**
	 * Get the set of properties for this communication channel.
	 */
	public Properties getProperties()
	{
		return _p; 
	}
	
	public ServerService() { }
	
	public void init() throws ServiceException
	{	
		if(!_p.containsKey("remoteValues")) throw new ServiceException("Field \"remoteValues\" must be set in properties file"); 
		if(!_p.containsKey("syncWaitingTime")) throw new ServiceException("Field \"syncWaitingTime\" not found. Please update conf file.");
		
		_isRemoteValuesSet = Boolean.parseBoolean(_p.getProperty("remoteValues"));

//		_sequencialLog = new SequencialLog(Long.parseLong(_p.getProperty("syncWaitingTime")));
		_sequencialLog = new SequencialLog();
		_clientLog = new ClientLog();
		_structuredLog = new StructuredLog();

/**		This was commented because I decided that no operation must be rejected based on time. Actually there are DBs that work much slower that others
//		and for that instance we can reject operation important to execute (See this more in detail). **/
//		long timeout = Long.parseLong(getProperties().getProperty("operationTimeout", Constants.DEFAULT_OPERATION_TIMEOUT));		
//		long threshold = Long.parseLong(getProperties().getProperty("operationThreshold", Constants.DEFAULT_OPERATION_THRESHOLD));
//		if(threshold < timeout) throw new ServiceException("init - Operation threshold must be equal or great than " +
//				"Operation timeout");
		
		_timer = new LogicalTimer((long)0);
//		_debug = SystemProperties._debug;
		
		try { 
			_db = DBFactory.newDB(getProperties());
			_db.init();
		} catch (Exception e) {
			throw new ServiceException(e); 
		}
	}
	
	public void cleanup() throws ServiceException
	{
		//TODO: write sequencial log to disk
//		getValues("data","k6",0,6); just for test
//		_structuredLog.deleteOldOperations(0, 4);
//		_clientLog.deleteOldOperations(0, 4);
//		_sequencialLog.deleteOldOperations(0, 4);
//		this.logToString();
		System.out.println("StructuredLogSize: "+_structuredLog.getSize());
		System.out.println("ClientLogSize: "+_clientLog.getSize());
		System.out.println("SequencialLogSize: "+_sequencialLog.getSize());
		
		try {
			_db.cleanup();
		} catch (DBException e) {
			throw new ServiceException(e);
		}
		finally	{
			_sequencialLog = null;
			_clientLog = null;
			_structuredLog = null;
			_timer = null;			
			_db = null;
		}
	}
	
//	/**
//	 * Compares if the client's timer differs to much from the server's.
//	 * @param op
//	 * @return
//	 */
//	public boolean verifyOperationTimeOut(long opTime) //TODO: See if this is right
//	{	
//		//boolean olderThanSyncPoint = false;/*_sequencialLog.getFirst().compareTo(op) > 0;*/  
//		return /*olderThanSyncPoint || _timer.verifyExceededTime(opTime);
//		//return false;
//	}
	
	/**
	 * The order phase allows us to known when the operation in argument should have appeared in the log, assuming that is
	 * delayed relatively to other operations, for the same family and row key. The timestamp assigned to each operation
	 * is used for the comparation.  
	 * 
	 * The transform phase is used to resolve possible conflicts that may occur between the operation in argument and all 
	 * others, for the same family and key, that were issued after in time and are already posted in the log. A conflict appears
	 * when an older issued operation tries to create/change/delete the database in the same fields created/changed/deleted 
	 * by newer ones, rising a possible lost update conflict. To avoid this problem, the older operation is transformed, 
	 * at each step, to a new compatible one when some conflict occurs. In this way it is possible to maximize the number of
	 * operations that can be executed.
	 * 
	 * In the execute phase, all operation that passed the two prior phases successfully are sent and executed in the associated DB. 
	 *     
	 * @param op
	 * @return
	 */
	public IResult orderTransformExecute(LoggableOperation op) throws ServiceException
	{
		ILogOperation newLogOp = null;
		if(_isRemoteValuesSet) newLogOp = op.convertToLog(_db);
		else newLogOp = op.convertToLog();
		
		//Atomic test to verify if this operation is the first to inserted in the log for some family and key.
		if(testAndAddNewOperation(newLogOp))
		{
			IResult result = this.executeUpdates(op);
//			System.out.println("Result Operation Code: "+result.getCode());
			return result;
		}
		// System.out.println("Row already exists...");
		LinkedList<ILogOperation> record = _structuredLog.get(newLogOp.getFamilyAndKey());
		
		IResult result = null;
		synchronized(record)
		{
			//System.out.println("OrderPhase initiated...");
			//Order phase 
			ListIterator<ILogOperation> it = _structuredLog.putOpInRecord(record, newLogOp);
			//System.out.println("OrderPhase completed...");
			
			Set<String> newLogOpFields = newLogOp.getFields();
			Set<String> opLogFields = null;
			ILogOperation opLog = null;
			int jumps = 0;
			//System.out.println("TransformationPhase initiated...");
			
			//TransformationPhase
			while(it.hasNext())
			{
//				System.out.println(opLog.toString());
				opLog = it.next();
//				System.out.println("CompareOp: " + opLog.toString());
				opLogFields = opLog.getFields();
				
				newLogOpFields.removeAll(opLogFields);  //SetA - SetB
				jumps++; //number of ops seen in the log
				
				//If the logOp set contains all elements of resultOp set, the resultOp can't be executed
				//and must be deleted from the log. A NoOp operation is returned.
				if(newLogOpFields.isEmpty())
				{
//					System.out.println("NOOP");
					while(jumps > -1){ jumps--; it.previous(); } //Change iterator pointer to the operation to remove
					it.remove(); //Remove resultOp from log
					result = new UpdateResult(0);
					break;
				}
				
//				if(opLogFields.containsAll(newLogOpFields))  //TODO: Remove NoOp Operation from log?
//				{
//					//System.out.println("Operation set is contained...");
////					System.out.println("NOOP");
//					while(jumps > 0){ jumps--; it.previous(); } //Change iterator pointer to the operation to remove
//					it.remove(); //Remove resultOp from log
//					result = new UpdateResult(0);
//					break;
//				}
//				else //All operations (Put and Delete) are transformed in the same way
//				{
//					//System.out.println("Operation must be transformed...");
//					jumps++;
//					newLogOpFields.removeAll(opLogFields); //SetA - SetB
//				}
			}
			
			//System.out.println("TransformationPhase done...");
			
			it = null;
			opLogFields = null;
			
			//only if a result hasn't been reached in this phase, the operation must be executed against a DB
			//System.out.println("ExecutePhase initializing...");
			if(result == null)
			{
				op.updateFields(newLogOpFields);
//				System.out.println("Inserting Client Log");
				_clientLog.put(op.getID(), newLogOp);
//				System.out.println("Inserting Sequencial Log");
				_sequencialLog.put(newLogOp);
//				System.out.println("Done Inserting Sequencial log");
				result = this.executeUpdates(op);
			}
//			System.out.println("Result Operation Code: "+result.getCode());
		}
		return result;
	}
	
	/**
	 * Test an set type method, that tests if a certain family and row key pair exists in the log and if not creates
	 * a new entry on it.
	 * 
	 * @param op
	 * @return
	 * @throws ServiceException
	 */
	public synchronized boolean testAndAddNewOperation(ILogOperation op)
	{
		Pair<String, String> familyAndKey = op.getFamilyAndKey();
		if(!_structuredLog.containsKey(familyAndKey))
		{
			LinkedList<ILogOperation> newRecord = new LinkedList<ILogOperation>();
//			ILogOperation opLog = null;
//			if(_isRemoteValuesSet) opLog = op.convertToLog(_db);
//			else opLog = op.convertToLog();
//			try {
//				System.out.println("OpLog size: "+opLog.serialize().length);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			newRecord.addLast(op);
			_structuredLog.put(familyAndKey, newRecord);
			_clientLog.put(op.getID(), op);
			_sequencialLog.put(op);
			return true;
		}
		return false;
	}
	
//	/**
//	 * Method passed to the operations
//	 *
//	 * After transforming an operation the final set of fields that not conflict with new operation log entries is 
//	 * obtained. With this auxiliar method the values/fields of the original operation are substituted by fields 
//	 * in argument to preserve transformation.  
//	 * 
//	 * @param op
//	 * @param fields
//	 * @return
//	 */
//	private Operation removeOldOperationFields(Operation op, Set<String> fields)
//	{
//		HashMap<String, String> finalValues = new HashMap<String,String>();
//		if(op instanceof Put)
//		{
//			HashMap<String, String> values = ((Put) op).getValues();
//			Iterator<String> itFields = fields.iterator();
//			while(itFields.hasNext())
//			{
//				String field = itFields.next();
//				finalValues.put(field, values.get(field));
//			}
//			((Put) op).setValues(finalValues);
//			return op;
//		}
//		else
//		{
//			((Delete) op).setFields(fields);
//			return op;
//		}
//	}
	
	public IResult executeUpdates(LoggableOperation op)
	{
//		System.out.println("Executing Operation...");
//		System.out.println(op.toString());
		UpdateResult result = null;
		int opType = op.getType();
//		System.out.println("OpType: "+opType);
		if(opType == 1)
			result = _db.put(op.getColumnFamily(), op.getRowKey(), op.getFieldsOrValues().getValues(), 
				getTime(), op.getConsistency());
		else if(opType == 2)
			result = _db.delete(op.getColumnFamily(), op.getRowKey(), op.getFieldsOrValues().getFields(), 
				getTime(), op.getConsistency());
		return result;
	}
	
	/**
	 * Executes an operation against the DB
	 * @param op
	 * @return
	 */
	public IResult executeReads(IClientOperation op) //How to change this? Already did by creating to different methods for executing reads and writes.
	{
		//System.out.println(op.toString());
		IResult result = null;
		int opType = op.getType();
		if(opType == 3)
			result =  _db.read(op.getColumnFamily(), op.getRowKey(), op.getFieldsOrValues().getFields(), 
					op.getConsistency());
		else if(opType == 4)
			result = _db.scan(op.getColumnFamily(), op.getRowKey(), ((Scan) op).getRecordCount(), //How to change this cast?
					op.getFieldsOrValues().getFields(), op.getConsistency());
		return result;
	}
	
	public long getTime() 
	{
		return _timer.getTime(); 
	}
	
//	public void setTime(long time)
//	{
//		_timer.setNewTime(time);
//	}
	
	public void deleteOldOperationsFromLog(long endTimestamp)
	{
		_structuredLog.deleteOldOperations(endTimestamp);
		_clientLog.deleteOldOperations(endTimestamp);
		_sequencialLog.deleteOldOperations(endTimestamp);
	}
	
	public Map<Integer,List<ILogOperation>> getSyncOperations(Map<Integer,Long> requestList)
	{
		HashMap<Integer,List<ILogOperation>> syncList = new HashMap<Integer,List<ILogOperation>>(requestList.size());
		for(Map.Entry<Integer,Long> pair : requestList.entrySet())
			syncList.put(pair.getKey(), _clientLog.getOperations(pair.getKey(), pair.getValue()));
		return syncList;
	}
	
	public boolean applyOperation(ILogOperation op)
	{
//		boolean success = false;
//		System.out.println("Family: "+op.getFamilyAndKey().get_value1()+", Key: "+op.getFamilyAndKey().get_value2());
//		boolean newFamilyAndKey = this.testAndAddNewOperation(op);
//		if(!newFamilyAndKey)
//		{
//			System.out.println("Not a new family for operation: "+op.getTS());
//			_structuredLog.putInRecord(op);
//			_clientLog.put(op.getID(), op);
//			_sequencialLog.put(op);
//		}
//		IResult res = this.executeUpdates(op);
//		System.out.println("Result code: "+res.getCode());
		
		IResult res = null;
		try {
			res = this.orderTransformExecute(op);
			
			//Unset Fields or Values (depending on the operation in question) because their no longer needed (just for sync).
			op.unsetFieldsOrValues();
			
			if(res.getCode() >= 0) return true; 
			return false;
		} catch (ServiceException e) {
			System.out.println("ServerService - Method applyOperation thrown a ServiceException");
			return false;
		}
	}
	
	//============================================================================//
	//							Testing Area									  //
	//============================================================================//
	
	private void toStringValues(Map<String,String> mapValues)
	{
		System.out.println("SizeMap: "+mapValues.size());
//		String values = " ";
		Iterator<Map.Entry<String, String>> it = mapValues.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<String, String> entry = it.next();
			System.out.print(entry.getKey() + "=" + entry.getValue() + ", ");
		}
		System.out.println("\n");
	}
	
	public void getValues(String columnFamily, String rowkey, long startTs, long endTs)
	{
		logToString();
		Iterator<ILogOperation> it = _structuredLog.get(new Pair<String,String>(columnFamily,rowkey)).iterator();
		while(it.hasNext())
		{
			ILogOperation op = it.next();
			if(op instanceof PutLog && op.getTS() >= startTs && op.getTS() <= endTs)
			{
				System.out.println("=======================================");
				System.out.println("Operation with ts: "+op.getTS());
				Map<String,String> map = ((PutLog)op).getValues();
				toStringValues(map);
				System.out.println("=======================================");
			}
		}
	}
	
	public void logToString()
	{
		System.out.println("=================================================================");
		System.out.println("||                          Log Read                           ||");
		System.out.println("=================================================================");
		System.out.println("");
		System.out.println(_structuredLog.toString());
		System.out.println("");
		System.out.println(_clientLog.toString());
		System.out.println("");
		System.out.println(_sequencialLog.toString());
	}
	
}
