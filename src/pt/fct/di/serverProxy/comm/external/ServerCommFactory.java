package pt.fct.di.serverProxy.comm.external;

import java.util.Properties;

import pt.fct.di.serverProxy.comm.UnknownCommException;
import pt.fct.di.util.Constants;

/**
 * ServerCommFactory is a generic factory to instantiate a new communication stack by dynamically 
 * classloading the specified communication class.
 * 
 * @author andre_goncalves@di
 *
 */
public class ServerCommFactory 
{	
	public static IServerComm newComm(Properties properties) throws UnknownCommException
    {
    	ClassLoader classLoader = ServerCommFactory.class.getClassLoader();

    	IServerComm ret=null;

		try 
		{
		   String commname = properties.getProperty("externalComm", Constants.DEFAULT_SERVER_COMM_STACK);
		   Class commclass = classLoader.loadClass(commname);
		   //System.out.println("dbclass.getName() = " + dbclass.getName());
		    
		   ret=(IServerComm)commclass.newInstance();
		 }
		 catch (Exception e) 
		 { 
		   UnknownCommException uce = new UnknownCommException(e.getMessage());
		   uce.setStackTrace(e.getStackTrace());
		   throw uce;
		 }
		 
		 ret.setProperties(properties);
		 return ret;
    }
}
