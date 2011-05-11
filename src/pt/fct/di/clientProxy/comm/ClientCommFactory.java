package pt.fct.di.clientProxy.comm;

import java.util.Properties;

import pt.fct.di.util.Constants;

/**
 * ClientCommFactory is a generic factory to instantiate a new communication stack
 * 
 * @author andre_goncalves@di
 *
 */
public class ClientCommFactory 
{
	//private static final String DEFAULT_COMM_STACK = "pt.fct.di.clientProxy.comm.TestClientComm";
	
	@SuppressWarnings("unchecked")
	public static ClientComm newComm(Properties properties) throws UnknownCommException
    {
    	ClassLoader classLoader = ClientCommFactory.class.getClassLoader();

    	ClientComm ret=null;

		try 
		{
		   String commname = properties.getProperty("commname", Constants.DEFAULT_CLIENT_COMM_STACK);
		   Class commclass = classLoader.loadClass(commname);
		   //System.out.println("dbclass.getName() = " + dbclass.getName());
		    
		   ret=(ClientComm)commclass.newInstance();
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
