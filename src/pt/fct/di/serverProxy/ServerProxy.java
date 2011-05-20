package pt.fct.di.serverProxy;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Properties;

import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.serverProxy.comm.UnknownCommException;
import pt.fct.di.serverProxy.comm.external.IServerComm;
import pt.fct.di.serverProxy.comm.external.ServerCommFactory;
import pt.fct.di.serverProxy.comm.internal.CommFactory;
import pt.fct.di.serverProxy.comm.internal.IComm;
import pt.fct.di.serverProxy.service.MessageDispatcher;
import pt.fct.di.serverProxy.service.ServerService;
import pt.fct.di.serverProxy.service.ServiceException;
import pt.fct.di.serverProxy.sync.SyncException;
import pt.fct.di.serverProxy.sync.SyncManager;

/**
 * This class represents a proxy in the server side to access the data base.
 * Implements the main daemon to initiate a new server proxy, service and 
 * communication stack.
 * 
 * TODO: Explain why ServerProxy extends Thread
 * 
 * @author andre_goncalves@di
 *
 */

public class ServerProxy extends Thread{

	/**
	 * @param args
	 */
	private IServerComm _externalcomm;
	private IComm _internalcomm;
	private MessageDispatcher _dispatcher;
	private SyncManager _manager;
	private ServerService _service;
	private Properties _p;
	
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
	
	
	public void init() throws SException
	{
		try
		{
			this._service = new ServerService();
			this._service.setProperties(_p);
			this._manager = new SyncManager();	
			this._manager.setProperties(_p);
			
			this._service.init();
			this._manager.init();
			
			this._dispatcher = new MessageDispatcher(_service, _manager);
			
			//Initiate external servers comunication
			this._externalcomm = ServerCommFactory.newComm(_p);
			this._externalcomm.setDispacher(_dispatcher);
			this._externalcomm.initServer();
			
			//Initiate internal servers communication
//			this._internalcomm = CommFactory.newComm(_p);
//			this._internalcomm.setDispacher(_dispatcher);
//			this._internalcomm.initServer();
			


		}
		 catch(UnknownCommException uce){
			throw new SException(uce);
		} catch(CommException ce){
			throw new SException(ce);
		} catch (ServiceException e) {
			throw new SException(e);
		} catch (SyncException e) {
			throw new SException(e);
		}
	}
	
	public void usageMessage()
	{
		
	}
	
	public void run()
	{
		try {
			closeServer();
		} catch (SException e) {
			e.printStackTrace();
		}
	}
	
	public void closeServer() throws SException
	{
		System.out.println("Shuting down server...");
		System.out.println("");
		System.out.println("------------------------------------------------------------------");
		System.out.println("");
		try {
			_service.cleanup();
		} catch (ServiceException e) {
			throw new SException(e);
		}
		finally
		{
			_dispatcher.close();
			_manager.cleanup();
			_externalcomm.cleanup();
			_internalcomm.cleanup();
		}
	}
	
	public static void main(String[] args) {
		System.out.println("ServerProxy is starting...");	
		try{
			int argindex = 0;
			
			ServerProxy proxy = new ServerProxy();
			Properties props = new Properties();
			Properties fileprops = new Properties();
			
			while( (argindex<args.length) && (args[argindex].startsWith("-")) )
			{
			    if ( (args[argindex].compareTo("-help")==0) ||
			   		 (args[argindex].compareTo("--help")==0) ||
			   		 (args[argindex].compareTo("-?")==0) ||
			   		 (args[argindex].compareTo("--?")==0) )
			   	    {
			   	       proxy.usageMessage();
			   	       System.exit(0);
			   	    }
			    else if (args[argindex].compareTo("-P")==0)
			    {
			       argindex++;
			       if (argindex>=args.length)
			       {
			    	   proxy.usageMessage();
			    	   System.exit(0);
			       }
			       String propfile=args[argindex];
			       argindex++;
			       
			       Properties myfileprops=new Properties();
			       try
			       {
			    	   myfileprops.load(new FileInputStream(propfile));
			       }
			       catch (IOException e)
			       {
			    	   System.out.println(e.getMessage());
			    	   System.exit(0);
			       }
			       
			       for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
			       {
			    	   String prop=(String)e.nextElement();
			    	   fileprops.setProperty(prop,myfileprops.getProperty(prop));
			       }
			       myfileprops = null;
			    }
			    else if (args[argindex].compareTo("-p")==0)
			    {
			       argindex++;
			       if (argindex>=args.length)
			       {
			    	   proxy.usageMessage();
			    	   System.exit(0);
			       }
			       int eq=args[argindex].indexOf('=');
			       if (eq<0)
			       {
			    	   proxy.usageMessage();
			    	   System.exit(0);
			       }
					   
			       String name=args[argindex].substring(0,eq);
			       String value=args[argindex].substring(eq+1);
			       props.put(name,value);
			       //System.out.println("["+name+"]=["+value+"]");
			       argindex++;
			    }
			    else
			    {
			       System.out.println("Unknown option "+args[argindex]);
			       proxy.usageMessage();
			       System.exit(0);
			    }

			    if (argindex>=args.length)
			    {
			       break;
			    }
			}
			
			String prop = null;
			//Copy all properties to a single instance
			for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
			{
			    prop=(String)e.nextElement();
			    fileprops.setProperty(prop,props.getProperty(prop));
			}
			 
			props=fileprops;
			prop = null;
			fileprops = null;
			
//		    Runtime.getRuntime().addShutdownHook(proxy);
			
			proxy.setProperties(props);
			proxy.init();
			
		    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		 
		    for (;;)
		    {
		    	//get user input
		    	System.out.print("> ");
		    
		    	String input=null;
		    
		    	try
		    	{
		    		input=br.readLine();
		    	}
		    	catch (IOException e)
		    	{
		    		e.printStackTrace();
		    		System.exit(1);
		    	}

		    	if (input.compareTo("")==0) 
		    	{
		    		continue;
		    	}
		    
		    	if (input.compareTo("quit")==0)
		    	{
		    		proxy.closeServer();
		    		break;
		    	}
		    }
		}
		catch(SException se)
		{
			se.printStackTrace();
		}
	}
}
