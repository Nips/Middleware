package pt.fct.di.multicastTCP;

/**
 * Generic Exception class for Client
 * @author Andre Goncalves
 *
 */

public class TransportException extends Exception{
	
	private static final long serialVersionUID = 1L;

	public TransportException(){
		super();
	}
	
	public TransportException(String message) {
	    super(message);
	  }

	  public TransportException(Throwable cause) {
	    super(cause);
	  }

	  public TransportException(String message, Throwable cause) {
	    super(message, cause);
	  }
}