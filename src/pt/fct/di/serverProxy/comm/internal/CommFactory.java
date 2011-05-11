package pt.fct.di.serverProxy.comm.internal;

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
public class CommFactory 
{
	public static IComm newComm(Properties properties) throws UnknownCommException
    {
    	ClassLoader classLoader = CommFactory.class.getClassLoader();

    	IComm ret=null;

		try 
		{
		   String commname = properties.getProperty("internalComm", Constants.DEFAULT_SERVER_COMM_STACK);
		   Class commclass = classLoader.loadClass(commname);
		   //System.out.println("dbclass.getName() = " + dbclass.getName());
		    
		   ret=(IComm)commclass.newInstance();
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
