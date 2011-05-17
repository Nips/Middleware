package pt.fct.di.clientProxy.service;

//import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
//import java.util.concurrent.TimeUnit;

import pt.fct.di.db.ConsistencyLevel;

import pt.fct.di.client.CException;
import pt.fct.di.clientProxy.comm.ClientComm;
import pt.fct.di.clientProxy.comm.ClientCommFactory;
import pt.fct.di.ops.*;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.util.SystemProperties;
import pt.fct.di.clientProxy.net.OneResponseHandler;
import pt.fct.di.clientProxy.net.QuorumResponseHandler;
import pt.fct.di.clientProxy.service.fields.SequencerId;
//import pt.fct.di.clientProxy.service.fields.time.LogicalTimer;
import pt.fct.di.clientProxy.service.fields.time.Timer;
import pt.fct.di.clientProxy.service.fields.time.VersionVector;

/**
 * ProxyService implements the service operating in the client proxy.
 * 
 * Here is maintained information about the last timestamp and message sequence id known by the client. For a new
 * message to send each parameter is incremented by one and added to the message.
 * 
 * It offers two types of methods: Asynchronous and Synchronous. Asynchronous methods are those that
 * execute in the server with no guarantees of success and the servers do not reply. Synchronous methods 
 * waits for the servers to respond and returns the answer to the client. If some unexpected event occurs
 * an exception is thrown with a message containing the cause.  
 * 
 * @author andre_goncalves@di
 *
 */

public class ProxyService implements IService{
	
	private ClientComm _comm = null;
	
	/**
	 * Controls the assignment of timestamps.
	 */
	private Timer _timer = null;
	
	/**
	 * Attributes a new id to an operation on a sequence order starting at 1.
	 */
	private SequencerId _seqId;
	
	/**
	 * Client's unique Identifier
	 */
	private int _clientId;
	
	/**
	 * Number of servers that the client connects.
	 */
	private int _nServers;
	
	public static ProxyService _ownInstance = new ProxyService();
	
	private boolean _debug;

//	private long _timestamp; //last timestamp known by the client
//	private int _nextSeq; //seq id to attribute to the next operation
	
//	public ProxyService(ClientComm comm){
//		this._comm = comm;
//	}
	
	public static ProxyService getInstance() throws CException
	{
		synchronized(_ownInstance)
		{
			if(_ownInstance._comm == null)
			{
				System.out.println("New Instance");
				_ownInstance.init();
			}
			return _ownInstance;
		}
	}
	
	/**
	 * Initialize client proxy service params
	 * 
	 * @throws CException
	 */
	public void init() throws CException
	{
//		Properties properties = SystemProperties.getProperties();
		try {
			_clientId = Integer.parseInt(SystemProperties.testAndGetPropertie("clientId"));
			_comm = ClientCommFactory.newComm(SystemProperties.getProperties());
			_comm.init();
		} catch (Exception e) {
			throw new CException(e);
		}
		_timer = new VersionVector();
		_seqId = SequencerId.getInstance();

		_nServers = _comm.getNumberServers();
		_comm.setService(this);
		_debug = SystemProperties._debug;
	}
	
	/**
	 * Cleanup the state of the service and params.
	 */
	public void cleanup()
	{
		long[] vv = _timer.getTimeVector();
		System.out.print("FinalVersionVector: [ ");
		for(int pos = 0; pos < vv.length; pos++)
			System.out.print(vv[pos]+", ");
		System.out.println("]");
//		/*if(_debug)*/ System.out.println("Closing ProxyService...");
		_comm.cleanup();
		//_timer = null;
		//_seqId = null;
//		/*if(_debug)*/ System.out.println("ProxyService closed...");
	}
	
//	/*++++++++++++++++ Auxiliary methods +++++++++++++++++*/
//	
//	private void setTimestamp(long ts1, long ts2)
//	{
//		this._timestamp = Math.max(ts1, ts2);
//	}
//	
//	private String nextSeq()
//	{
//		String toReturn = Integer.toString(_nextSeq); 
//		_nextSeq++;
//		return toReturn;
//	}
	
	public void setNumberofServers(int nservers)
	{
		_nServers = nservers;
	}
	
	public void updateTimeVector(long[] otherVector) throws Exception
	{
		_timer.updateTimer(otherVector);
//		System.out.println("Clock: "+_timer.getTimeVector()[_clientId]);
	}
	
	public long[] getTimeVector()
	{
		return _timer.getTimeVector();
	}
	
	public int getNextSeqNum()
	{
		return _seqId.getNextId();
	}
	
	public int getId()
	{
		return _clientId;
	}
	
    private int determineBlockFor(ConsistencyLevel consistency_level)
    {
        switch (consistency_level)
        {
            case ONE:
            case ANY:
                return 1;
            case QUORUM:
                return (_nServers / 2) + 1;
            case ALL:
                return _nServers;
            default:
                throw new UnsupportedOperationException("invalid consistency level " + consistency_level);
        }
    }
	
//	/*++++++++++++++++++++++++++++++++++++++++++++++++++++*/
	
//	@SuppressWarnings("unchecked")
	public int read(String column_family, String row_key, Set<String> fields, HashMap<String, String> resultMap, 
			ConsistencyLevel level) throws CException
	{
		int id = -1;
		try
		{
			Read readOperation = new Read(getId(), getNextSeqNum(), column_family, row_key, fields, level);
//			readOperation.argsValidation();
			
			ReadResult result = null;
			int blockFor = this.determineBlockFor(readOperation.getConsistency());
			if(blockFor == ConsistencyLevel.ONE.getValue())
			{
				OneResponseHandler handler = new OneResponseHandler();
				id = _comm.sendReadMessage(readOperation, handler);
//				readOperation.destroyOp();
				readOperation = null;
//				result = handler.get(SystemProperties.getRpcTimeout(), TimeUnit.MILLISECONDS);
				result = (ReadResult)handler.get();
				ClientComm.removeRegisteredCallback(id);
			}
			else
			{
				QuorumResponseHandler handler = new QuorumResponseHandler(blockFor);
				id = _comm.sendMessage(readOperation, handler);
//				readOperation.destroyOp();
				readOperation = null;
				result = (ReadResult)handler.get();
				ClientComm.removeRegisteredCallback(id);		
			}
			
//			if(result == null) return -2;
			
//			_timer.setAndUpdateTime(result.getVersionVector(), //por ID); //TODO: Verificar se isto devia estar aqui ou no TransportLayer
			
			int code = result.getCode();
			if(code < 0 ) return code;
			
			//read results
			for(Map.Entry<String, String> ent: ((HashMap<String, String>) result.getValues()).entrySet())
			{
				resultMap.put(ent.getKey(), ent.getValue());
			}
//			result.destroyResult();
			result = null;

			return code;
		}catch(Exception e){
			System.out.println("Read Exception...");
			if(id > -1) ClientComm.removeRegisteredCallback(id);
			throw new CException(e); 
		}
	}
	
	public int scan(String column_family, String start_row_key, int recordcount, Set<String> fields, 
			Vector<HashMap<String, String>> result, ConsistencyLevel level) throws CException
	{
		/*setTimestamp(scanOperation);
		Result scanRes = _comm.sendMessageReceive(scanOperation);
		_timer.setNewTime(scanRes.getTS());
		return scanRes;*/
		return 0;
//		return new Result("Scan", scanOperation.getID(), -4, "TestArea", getTimestamp());
	}
	
	public int putAsync(String column_family, String row_key, HashMap<String, String> values) throws CException
	{
		try
		{
			Put insertOperation = new Put(getId(), getNextSeqNum(), column_family, row_key, values, ConsistencyLevel.ZERO);
//			insertOperation.argsValidation();
			
//			insertOperation.setID(_clientId);
//			insertOperation.setOpSeq(getNextSeqNum());
			_comm.sendOneWay(insertOperation);
//			insertOperation.destroyOp();
//			insertOperation = null;
			return 0;
		
		}catch (Exception e) {
			throw new CException(e);
		}
	}
	
	public int putSync(String column_family, String row_key, HashMap<String, String> values, 
			ConsistencyLevel level) throws CException
	{	
		int id = -1;
		try
		{
			Put putOperation = new Put(getId(), getNextSeqNum(), column_family, row_key, values, level);
//			putOperation.argsValidation();
			
			UpdateResult result = null;
			int blockFor = this.determineBlockFor(putOperation.getConsistency());
			if(blockFor == ConsistencyLevel.ONE.getValue())
			{
				OneResponseHandler handler = new OneResponseHandler();
				id = _comm.sendMessage(putOperation, handler);
//				putOperation.destroyOp();
				putOperation = null;
//				result = handler.get(SystemProperties.getRpcTimeout(), TimeUnit.MILLISECONDS);
				result = (UpdateResult)handler.get();
				ClientComm.removeRegisteredCallback(id);
			}
			else
			{
				QuorumResponseHandler handler = new QuorumResponseHandler(blockFor);
				id = _comm.sendMessage(putOperation, handler);
//				putOperation.destroyOp();
				putOperation = null;
				result = (UpdateResult)handler.get();
				ClientComm.removeRegisteredCallback(id);
			}
			
//			if(result == null) return -2;
			
			int code = result.getCode();
//			_timer.setNewTime(result.getTS()); NOW ALL RECEIVED MESSAGES UPDATES CLIENT TIME. IT IS DONE IN THE TRANSPORT LAYER!!!!
//			result.destroyResult();
			result = null;
	
			return code;
			
		}catch(Exception e){ 
			System.out.println("Put Exception...");
			if(id > -1) ClientComm.removeRegisteredCallback(id);
			throw new CException(e); 
		 }
		
//		int returnedCode = result.getCode(); 
//		if(returnedCode >= 0) return returnedCode;
//		else throw new CException("Error code "+returnedCode+": "+result.getMsg());
	}
	
//	public void updateAsync(Update updateOperation) throws CException
//	{	
//		setFields(updateOperation);
//		_comm.sendMessage(updateOperation);
//	}
//	
//	public void updateSync(Update updateOperation) throws CException
//	{	
//		setFields(updateOperation);
//		Result result = _comm.sendMessageReceive(updateOperation);
//		if(result.getCode() >= 0)
//		{
//			setTimestamp(result.getTS());
//			return;
//		}
//		else
//			throw new CException(result.getMsg());
//	}
	
	public int removeAsync(String column_key, String row_key, Set<String> fields) throws CException
	{	
		try
		{
			Delete deleteOperation = new Delete(getId(), getNextSeqNum(), column_key, row_key, fields, ConsistencyLevel.ZERO);
//			deleteOperation.argsValidation();
			
//			deleteOperation.setID(_clientId);
//			deleteOperation.setOpSeq(getNextSeqNum());
			_comm.sendOneWay(deleteOperation);
//			deleteOperation.destroyOp();
			return 0;
		}catch(Exception e){ 
			throw new CException(e); }
	}
	
	public int removeSync(String column_key, String row_key, Set<String> fields, ConsistencyLevel level) throws CException
	{	
		int id = -1;
		try
		{
			Delete deleteOperation = new Delete(getId(), getNextSeqNum(), column_key, row_key, fields, level);
//			deleteOperation.argsValidation();
			
			UpdateResult result = null;
			int blockFor = this.determineBlockFor(deleteOperation.getConsistency());
			if(blockFor == ConsistencyLevel.ONE.getValue())
			{
				OneResponseHandler handler = new OneResponseHandler();
				id = _comm.sendMessage(deleteOperation, handler);
//				deleteOperation.destroyOp();
				deleteOperation = null;
//				result = handler.get(SystemProperties.getRpcTimeout(), TimeUnit.MILLISECONDS);
				result = (UpdateResult)handler.get();
				ClientComm.removeRegisteredCallback(id);
			}
			else
			{
				QuorumResponseHandler handler = new QuorumResponseHandler(blockFor);
				id = _comm.sendMessage(deleteOperation, handler);
//				deleteOperation.destroyOp();
				deleteOperation = null;
				result = (UpdateResult)handler.get();
				ClientComm.removeRegisteredCallback(id);
			}
			
//			if(result == null) return -2;
			
			int code = result.getCode();
//			_timer.setNewTime(result.getTS());
//			result.destroyResult();
			result = null;
			
			return code;
			
		}catch(Exception e){
			if(id > -1) ClientComm.removeRegisteredCallback(id);
			throw new CException(e); }
		
//		else throw new CException("Error code "+returnedCode+": "+result.getMsg());
	}
}
