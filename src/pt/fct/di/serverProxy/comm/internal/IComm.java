package pt.fct.di.serverProxy.comm.internal;

import java.util.Properties;

//import pt.fct.di.ops.Operation;
//import pt.fct.di.serverProxy.concurrent.ThreadPool;
//import pt.fct.di.serverProxy.service.MessageBuffer;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.serverProxy.service.MessageDispatcher;
import pt.fct.di.util.Constants;

/**
 * IComm is an abstraction of a communication stack.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * @author andre_goncalves@di
 *
 */

public abstract class IComm{
	
	/**
	 * Properties for configuring this communication channel.
	 */
	private Properties _p=new Properties();
	
	/**
	 * Dispacher to treat all recognized messages to the server
	 */
	public static MessageDispatcher _dispatcher;

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
	public abstract void initServer() throws CommException;
	
	/**
	 * Cleanup any state and close open channels.
	 */
	public abstract void cleanup();
	
	/**
	 * SetService - Set a new instance of the server service
	 * @param service
	 */
	public void setDispacher(MessageDispatcher dispatcher)
	{
		this._dispatcher = dispatcher;
	}
	
//	/**
//	 * GetService - Get instance of the server service
//	 * @return service
//	 */
//	public MessageDispatcher getDispatcher()
//	{
//		return this._dispatcher;
//	}
}
