package pt.fct.di.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import pt.fct.di.client.interfaces.ClientInterface;
//import pt.fct.di.client.interfaces.CommandLine;
import pt.fct.di.client.interfaces.TestSite;
import pt.fct.di.clientProxy.comm.UnknownCommException;

/**
 * This class represents a general client to access the data base.
 * Implements the main daemon to initiate a new client. 
 * 
 * @author andre_goncalves@di
 *
 */
public class Client extends Thread{
		
	/**
	 * Properties to define client choices, e.g communication stack to use. 
	 */
	private Properties _p;
	
	private ClientInterface _cmd = null;
	
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
	
	public void init() throws CException, UnknownCommException
	{
		//this._cmd = new CommandLine();
		this._cmd = new TestSite();
		_cmd.setProperties(_p);
		_cmd.init();
	}

	public void usageMessage()
	{
		
	}
	
	public void run()
	{
		closeClient();
	}
	
	public void closeClient()
	{
		System.out.println("Shuting down client...");
		_cmd.cleanup();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int argindex = 0;
		
		Client cl = new Client();
		Properties props = new Properties();
		Properties fileprops = new Properties();
		
		while( (argindex<args.length) && (args[argindex].startsWith("-")) )
		{
		    if ( (args[argindex].compareTo("-help")==0) ||
		   		 (args[argindex].compareTo("--help")==0) ||
		   		 (args[argindex].compareTo("-?")==0) ||
		   		 (args[argindex].compareTo("--?")==0) )
		    {
		    	cl.usageMessage();
		   	   	System.exit(0);
		   	}
		    else if (args[argindex].compareTo("-P")==0)
		    {
		       argindex++;
		       if (argindex>=args.length)
		       {
		    	   cl.usageMessage();
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
		    }
		    else if (args[argindex].compareTo("-p")==0)
		    {
		       argindex++;
		       if (argindex>=args.length)
		       {
		    	   cl.usageMessage();
		    	   System.exit(0);
		       }
		       int eq=args[argindex].indexOf('=');
		       if (eq<0)
		       {
		    	   cl.usageMessage();
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
		       cl.usageMessage();
		       System.exit(0);
		    }

		    if (argindex>=args.length)
		    {
		       break;
		    }
		}
		
		//Copy all properties to a single instance
		for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
			String prop=(String)e.nextElement();
		    
			fileprops.setProperty(prop,props.getProperty(prop));
		}
		 
		props=fileprops;
		
		Runtime.getRuntime().addShutdownHook(cl);
		
		try
		{
			cl.setProperties(props);
			cl.init();
		} 
		catch (UnknownCommException uce)
		{
			uce.printStackTrace();
		}
		catch (CException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
}
