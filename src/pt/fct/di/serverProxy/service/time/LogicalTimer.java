package pt.fct.di.serverProxy.service.time;

import java.util.concurrent.atomic.AtomicLong;

public class LogicalTimer implements Timer{

	private AtomicLong _timer;
//	private long _timeout;
//	private long _threshold;
	
	public LogicalTimer(long initialtime)
	{
		_timer = new AtomicLong(initialtime);
	}
	
//	public LogicalTimer(long initialtime, long timeout, long threshold)
//	{
//		_timer = new Long(initialtime);
//		_timeout = timeout;
////		_threshold = threshold;
//		//System.out.println(_timer+", "+initialtime+", "+_timeout+", "+_threshold);
//	}
	
	public long getTime()
	{
		//_timer++;
		return _timer.incrementAndGet();
	}
	
	public synchronized void setNewTime(long newtimer)
	{
		//System.out.println("CurrentTime: "+_timer+", New time is: "+newtimer);
		_timer.set(Math.max(_timer.get(), newtimer));
	}
	
	@Override
	public synchronized void resetTimer(long initialTime) 
	{
		_timer.set(initialTime);
	}
	
	/**
	 * Verifies if a certain client's time, given in argument, differs too much from the server's. 
	 */
	public boolean verifyExceededTime(long time)
	{
//		long diff = Math.abs(_timer - time);
//		return diff >= _timeout /*|| diff >= _threshold*/;
		return false;
	}
}
