package pt.fct.di.clientProxy.comm;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Properties;

import pt.fct.di.clientProxy.net.IAsyncCallback;
import pt.fct.di.clientProxy.service.ProxyService;
import pt.fct.di.ops.IClientOperation;
//import pt.fct.di.ops.Operation;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.util.Constants;

/**
 * IClientComm is an abstraction of a communication stack.
 * 
 * Where we keep a record of the operations to synchronize with the server, in _syncOps, and 
 * a response queue, in _responseQueue, to store results returned from the server.
 * Only the results of operations that need to be synchronized are maintained in _responseQueue.
 * 
 * This class should be constructed using a no-argument constructor, so we can load it dynamically. 
 * Any argument-based initialization should be done by method init().
 * 
 * @author andre_goncalves@di
 *
 */
public abstract class ClientComm{
	
	//TODO: See if expiring map is necessary in this case? No because all callbacks remove itself from the map
	//when all waiting messages arrive or a associated timeout is reached. 
	
	/**
	 * Object to access to client service methods
	 */
	protected ProxyService _service;
	
	/**
	 * Leaves callbacks in the map long enough until relevant results from the servers arrive before
	 * the callback be removed from it. 
	 */
	protected static HashMap<Integer, IAsyncCallback> _callbackMap = new HashMap<Integer, IAsyncCallback>();
	
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
	
	public String getDefaultConfFile()
	{
		return Constants.DEFAULT_COMM_CONF_FILE;
	}
	
	/**
	 * Initialize communication stack.
	 */
	public abstract void init() throws CommException;
	
	/**
	 * Cleanup any state and close open channels.
	 */
	public abstract void cleanup();
	
	/**
	 * Get ip address given to this machine.
	 * @return
	 */
	public abstract String getAddr() throws UnknownHostException;
	
	/**
	 * Gets the ip address of the reader server.
	 */
	public abstract String getReaderAddr();
	
	/**
	 * Gets the number of servers that this client connects to.
	 * @return
	 */
	public abstract int getNumberServers();
	
	/**
	 * Sends a multicast message to a group of server proxies.
	 * Does not wait for any synchronous response from them.
	 * @param op
	 * @throws CommException 
	 */
	public abstract void sendOneWay(IClientOperation op) throws CommException;
	
	/**
	 * Sends a oni directional message to the addr in argument
	 * Does not wait for any synchronous response from it.
	 * @param op
	 * @throws CommException
	 */
	public abstract void sendOneWay(IClientOperation op, String addr) throws CommException;
	
	/**
	 * Sends a multicast message to a group of server proxies.
	 * Responses are received asynchronously. 
	 */
	public abstract int sendMessage(IClientOperation op, IAsyncCallback cb) throws CommException;
	
	/**
	 * Sends a oni directional read/scan message.
	 * Response is received asynchronously
	 * @param op
	 * @param addr
	 * @param cb
	 * @throws CommException
	 */
	public abstract int sendReadMessage(IClientOperation op, IAsyncCallback cb) throws CommException;
	
	public static void addCallback(IAsyncCallback cb, int operationSeqNum)
	{
		//cb.setId(messageId);
		_callbackMap.put(operationSeqNum, cb);
	}
	
    public static IAsyncCallback getRegisteredCallback(int key)
    {
        return _callbackMap.get(key);
    }
    
    public static void removeRegisteredCallback(int key)
    {
        IAsyncCallback cb = _callbackMap.remove(key);
        if(cb == null) System.out.println("Not contains callback for id "+key);
        cb = null;
    }
    
    public static int mapElems()
    {
    	return _callbackMap.size();
    }
    
    public void setService(ProxyService service)
    {
    	this._service = service;
    }

}
