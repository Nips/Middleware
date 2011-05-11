package pt.fct.di.serverProxy.comm;

public class CommException extends Exception{
	
	private static final long serialVersionUID = 5990269534481291134L;

	public CommException(String message) 
    {
		super(message);
    }
    
    public CommException()
    {
    	super();
    }

    public CommException(String message, Throwable cause)
    {
    	super(message,cause);
    }
    
    public CommException(Throwable cause)
    {
    	super(cause);
    }

}
