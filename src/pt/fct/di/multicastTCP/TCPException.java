package pt.fct.di.multicastTCP;

/**
 * Generic Exception class for Client
 * @author Andre Goncalves
 *
 */

public class TCPException extends Exception{
	
	private static final long serialVersionUID = 1L;

	public TCPException(){
		super();
	}
	
	public TCPException(String message) {
	    super(message);
	  }

	  public TCPException(Throwable cause) {
	    super(cause);
	  }

	  public TCPException(String message, Throwable cause) {
	    super(message, cause);
	  }
}