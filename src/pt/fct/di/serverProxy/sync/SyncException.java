package pt.fct.di.serverProxy.sync;

public class SyncException extends Exception{
	
	private static final long serialVersionUID = 5990269534481291134L;

	public SyncException(String message) 
    {
		super(message);
    }
    
    public SyncException()
    {
    	super();
    }

    public SyncException(String message, Throwable cause)
    {
    	super(message,cause);
    }
    
    public SyncException(Throwable cause)
    {
    	super(cause);
    }

}
