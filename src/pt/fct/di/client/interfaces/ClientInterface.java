package pt.fct.di.client.interfaces;

import java.util.Properties;

import pt.fct.di.client.CException;
import pt.fct.di.clientProxy.comm.UnknownCommException;

/**
 * Represents a general user interface 
 *  
 * @author owner
 *
 */
public interface ClientInterface {

	/**
	 * Set the properties.
	 */
	public void setProperties(Properties p);
	
	/**
	 * Get the set of properties.
	 */
	public Properties getProperties();
	
	/**
	 * Initialize Client Interface
	 * @throws CException
	 */
	public void init() throws CException, UnknownCommException;
	
	/**
	 * Clean up any state of client interface
	 */
	public void cleanup();
	
	/**
	 * Help method
	 */
	public void help(); 
	
}
