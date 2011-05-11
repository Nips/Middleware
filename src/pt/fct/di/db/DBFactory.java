package pt.fct.di.db;

import java.util.Properties;

import pt.fct.di.util.Constants;

/**
 * DBFactory is a generic factory to instantiate a new DB by dynamically classloading 
 * the specified DB class.
 * 
 * @author andre_goncalves@di
 *
 */
public class DBFactory
{
    @SuppressWarnings("unchecked")
	public static DB newDB(Properties properties) throws UnknownDBException
    {
    	  ClassLoader classLoader = DBFactory.class.getClassLoader();

    	  DB ret=null;

		 try 
		 {
			String dbname = properties.getProperty("dbname", Constants.DEFAULT_DB_NAME);
		    Class dbclass = classLoader.loadClass(dbname);
		    //System.out.println("dbclass.getName() = " + dbclass.getName());
		    
		    ret=(DB)dbclass.newInstance();
		 }
		 catch (Exception e) 
		 {  
		   UnknownDBException ude = new UnknownDBException(e.getMessage());
		   ude.setStackTrace(e.getStackTrace());
		   throw ude;
		 }
		 
		 ret.setProperties(properties);
		 return ret;
    } 
}
