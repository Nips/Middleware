package pt.fct.di.clientProxy.service.fields.time;

/**
 * This auxiliar interface represents a internal server timer.
 * @author andre_goncalves@di
 *
 */
public interface Timer {

	public long[] getTimeVector();
	public void updateOwnTime(int id);
	public void updateTimer(long[] otherVector) throws Exception;
	public void setAndUpdateTime(long[] otherVector, int id) throws Exception;
	public void resetTimer();
	
}
