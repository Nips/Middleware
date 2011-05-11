package pt.fct.di.serverProxy;

/**
 * Generic Exception class for Client
 * @author Andre Goncalves
 *
 */

public class SException extends Exception{
	
	private static final long serialVersionUID = 1L;

	public SException(){
		super();
	}
	
	public SException(String message) {
	    super(message);
	  }

	  public SException(Throwable cause) {
	    super(cause);
	  }

	  public SException(String message, Throwable cause) {
	    super(message, cause);
	  }
}