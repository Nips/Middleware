package pt.fct.di.client.interfaces;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
public class CommandLine implements ClientInterface{
	
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
		 System.out.println("Middleware Command Line client");
		 System.out.println("Type \"help\" for command line help");
		 System.out.println("Start with \"-help\" for usage info");
		 
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

		    if (input.compareTo("help")==0) 
		    {
		       help();
		       continue;
		    }

		    if (input.compareTo("quit")==0)
		    {
		    	_db.cleanup();
		    	break;
		    }
		    
		    String[] tokens=input.split(" ");
		    
		    long st=System.currentTimeMillis();
		    
		    //handle commands
		    if (tokens[0].compareTo("read") == 0)
		    {
		    	if(tokens.length!=3)
		    		System.err.println("Error: sintax is \"read objectID consistencyLevel\"");
		    	else
		    	{
		    		String objectID = tokens[1];
		    		String level = tokens[2];
		    		ConsistencyLevel c = getLevel(level);
		    		if(c != null)
		    			_db.read(objectID, c);
		    		else
		    			System.err.println("Cmd - Consistency Level "+c+" not found");
		    	}
		    }
		    else if (tokens[0].compareTo("scan") == 0)
		    {
		    	System.out.println("The command selected is scan");
		    }
		    else if(tokens[0].compareTo("insert") == 0)
		    {
		    	if(tokens.length!=4)
		    		System.err.println("Error: sintax is \"insert objectID value consistencyLevel\"");
		    	else
		    	{
		    		String objectID = tokens[1];
		    		int value = Integer.parseInt(tokens[2]);
		    		String level = tokens[3];
		    		ConsistencyLevel c = getLevel(level);
		    		if(c != null)
		    			_db.insert(objectID, value, c);
		    		else
		    			System.err.println("Cmd - Consistency Level "+c+" not found");
		    	}
		    }
		    else if(tokens[0].compareTo("update") == 0)
		    {
		    	if(tokens.length!=4)
		    		System.err.println("Error: sintax is \"update objectID value consistencyLevel\"");
		    	else
		    	{
		    		String objectID = tokens[1];
		    		int value = Integer.parseInt(tokens[2]);
		    		String level = tokens[3];
		    		ConsistencyLevel c = getLevel(level);
		    		if(c != null)
		    			_db.update(objectID, value, c);
		    		else
		    			System.err.println("Cmd - Consistency Level "+c+" not found");
		    	}
		    }
		    else if(tokens[0].compareTo("delete") == 0)
		    {
		    	if(tokens.length!=3)
		    		System.err.println("Error: sintax is \"remove objectID consistencyLevel\"");
		    	else
		    	{
		    		String objectID = tokens[1];
		    		String level = tokens[2];
		    		ConsistencyLevel c = getLevel(level);
		    		if(c != null)
		    			_db.remove(objectID, c);
		    		else
		    			System.err.println("Cmd - Consistency Level "+c+" not found");
		    	}
		    }
		    else
		    {
		       System.out.println("Error: unknown command \""+tokens[0]+"\"");
		    }
		    
		    System.out.println((System.currentTimeMillis()-st)+" ms");
		 }
	}
}
