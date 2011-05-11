package pt.fct.di.client.interfaces;

import java.util.Properties;

import pt.fct.di.client.CException;
import pt.fct.di.client.db.ClientDB;
import pt.fct.di.client.db.ConsistencyLevel;
import pt.fct.di.client.db.IClientDB;

/**
 * This class implements a simple command line where an user gives commands
 * to execute.
 * 
 * @author andre_goncalves@di
 *
 */
public class TestSite implements ClientInterface{
	
	/**
	 * Properties to define client choices, e.g communication stack to use. 
	 */
	private Properties _p;
	
	private IClientDB _db;
	
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
	
	public void init() throws CException
	{
		this._db = new ClientDB();
		this._db.setProperties(_p);
		this._db.init();
		start();
	}
	
	public void cleanup()
	{
		this._db.cleanup();
		this._db = null;
	}

	public void help()
	{
		 System.out.println("Commands:");
		 System.out.println("  read objectID - Read a record");
		 //System.out.println("  scan key recordcount [field1 field2 ...] - Scan starting at key");
		 System.out.println("  insert objectID value - Insert a new record");
		 System.out.println("  update objectID value - Update a record");
		 System.out.println("  delete objectID - Delete a record");
		 //System.out.println("  table [tablename] - Get or [set] the name of the table");
		 System.out.println("  quit - Quit");
	}
	
	private ConsistencyLevel getLevel(String level)
	{
		if(level.compareTo("ZERO") == 0) return ConsistencyLevel.ZERO;
		else if(level.compareTo("ANY") == 0) return ConsistencyLevel.ANY;
		else if(level.compareTo("ONE") == 0) return ConsistencyLevel.ONE;
		else if(level.compareTo("QUORUM") == 0) return ConsistencyLevel.QUORUM;
		else if(level.compareTo("ALL") == 0) return ConsistencyLevel.ALL;
		return null;
	}
	
	private void start()
	{	    
	    long st=System.currentTimeMillis();
	    
	    for(int i=0; i<100000; i++)
	    {
	    	_db.insert(Integer.toString(i), i, getLevel("ZERO"));
	    }
		    
		System.out.println((System.currentTimeMillis()-st)+" ms");
	}
}
