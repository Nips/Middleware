package pt.fct.di.serverProxy.comm.external;

import java.util.Properties;

//import pt.fct.di.ops.Operation;
//import pt.fct.di.serverProxy.concurrent.ThreadPool;
//import pt.fct.di.serverProxy.service.MessageBuffer;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.serverProxy.service.MessageDispatcher;
import pt.fct.di.util.Constants;

/**
 * IServerComm is an abstraction of a communication stack.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * @author andre_goncalves@di
 *
 */

public abstract class IServerComm{
	
	/*public class Message
	{
		private ISender _sender;
		private Operation _op;
		
		public Message(ISender sender, Operation op)
		{
			this._sender = sender;
			this._op = op;
		}
		
		public ISender getSender(){ return _sender; }
		
		public Operation getOp(){ return _op; }
	}*/
	
	/**
	 * Properties for configuring this communication channel.
	 */
	private Properties _p=new Properties();
	
	/**
	 * Dispacher to treat all recognized messages to the server
	 */
	public MessageDispatcher _dispatcher;
	
	
//	/**
//	 * Thread pool to store reusable threads for treat incoming messages
//	 */
//	public ThreadPool _pool = null;
	
//	/**
//	 * Buffer to store all waiting operations to execute
//	 */
//	public MessageBuffer _buffer = null;

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
	
/*	public void init()
	{
		initPool();
		//initMsgBuffer();
	}
	
	public void initPool()
	{
		int minThreads = Integer.parseInt(getProperties().getProperty("minPoolThreads", 
				Constants.DEFAULT_MIN_POOL_THREADS));
		int maxThreads = Integer.parseInt(getProperties().getProperty("maxPoolThreads", 
				Constants.DEFAULT_MAX_POOL_THREADS));
		long timeToLive = Long.parseLong(getProperties().getProperty("threadTimeToLive", 
				Constants.DEFAULT_THREAD_TIME_TO_LIVE));
		int queueSize = Integer.parseInt(getProperties().getProperty("msgQeueSize", 
				Constants.DEFAULT_MESSAGE_BUFFER_SIZE));
		
		this._pool = new ThreadPool(minThreads, maxThreads, timeToLive, queueSize);

	} */
	
	/*public void initMsgBuffer()
	{
		this._buffer = MessageBuffer.getInstance();
	}*/
	
	/**
	 * Initialize communication stack.
	 */
	public abstract void initServer() throws CommException;
	
	/**
	 * Cleanup any state and close open channels.
	 */
	public abstract void cleanup();

	
//	/**
//	 * get_buffer - Get the service that wraps a message buffer
//	 * @return
//	 */
//	public MessageBuffer get_buffer() {
//		return _buffer;
//	}
//
//	/**
//	 * set_buffer - Sets the service that wraps the message buffer 
//	 * @param _buffer
//	 */
//	public void set_buffer(MessageBuffer _buffer) {
//		this._buffer = _buffer;
//	}
	
	/**
	 * SetService - Set a new instance of the server service
	 * @param service
	 */
	public void setDispacher(MessageDispatcher dispatcher)
	{
		this._dispatcher = dispatcher;
	}
	
	/**
	 * GetService - Get instance of the server service
	 * @return service
	 */
	public MessageDispatcher getDispatcher()
	{
		return this._dispatcher;
	}
}
