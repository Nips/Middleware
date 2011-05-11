package pt.fct.di.ops;

/**
 * Generic Exception class for Client
 * @author Andre Goncalves
 *
 */

public class OpException extends Exception{
	
	private static final long serialVersionUID = 1L;

	public OpException(){
		super();
	}
	
	public OpException(String message) {
	    super(message);
	  }

	  public OpException(Throwable cause) {
	    super(cause);
	  }

	  public OpException(String message, Throwable cause) {
	    super(message, cause);
	  }
}