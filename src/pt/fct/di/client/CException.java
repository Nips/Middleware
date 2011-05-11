package pt.fct.di.client;

/**
 * Generic Exception class for Client
 * @author Andre Goncalves
 *
 */

public class CException extends Exception{
	
	private static final long serialVersionUID = 1L;

	public CException(){
		super();
	}
	
	public CException(String message) {
	    super(message);
	  }

	  public CException(Throwable cause) {
	    super(cause);
	  }

	  public CException(String message, Throwable cause) {
	    super(message, cause);
	  }
}