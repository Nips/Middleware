package pt.fct.di.serverProxy.service.time;

/**
 * This auxiliar interface represents a internal server timer.
 * @author andre_goncalves@di
 *
 */
public interface Timer {

	public long getTime();
	public void setNewTime(long timer);
	public void resetTimer(long initialTime);
	
	public boolean verifyExceededTime(long time);
}
