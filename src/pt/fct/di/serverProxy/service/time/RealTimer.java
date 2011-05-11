package pt.fct.di.serverProxy.service.time;

public class RealTimer implements Timer{

	private long _timeout;
	private long _threshold;
	
	public RealTimer(long timeout, long threshold)
	{ 
		_timeout = timeout;
		_threshold = threshold;
	}
	
	public long getTime()
	{
		return System.currentTimeMillis();
	}
	
	@Override
	public void setNewTime(long timer) {
		//Not Supported	
	}

	@Override
	public void resetTimer(long initialTime) {
		//Not Supported
	}
	
	/**
	 * Verifies if a certain client's time, given in argument, differs too much from the server's. 
	 */
	public boolean verifyExceededTime(long time)
	{
		long diff = Math.abs(getTime() - time);
		return diff >= _timeout || diff >= _threshold;
	}
}
