package pt.fct.di.serverProxy.comm;

public class UnknownCommException extends Exception{
	
	private static final long serialVersionUID = 5990269534481291134L;

	public UnknownCommException(String message) 
    {
		super(message);
    }
    
    public UnknownCommException()
    {
    	super();
    }

    public UnknownCommException(String message, Throwable cause)
    {
    	super(message,cause);
    }
    
    public UnknownCommException(Throwable cause)
    {
    	super(cause);
    }

}
