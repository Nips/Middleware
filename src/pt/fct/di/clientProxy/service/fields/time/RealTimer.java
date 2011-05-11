package pt.fct.di.clientProxy.service.fields.time;

public class RealTimer implements Timer{
	
	private Long _delay = (long)0;
	
	public RealTimer()
	{ 
	}

	@Override
	public long[] getTimeVector() {
		long[] clock = new long[1];
		clock[0] = System.currentTimeMillis()+_delay;
		return clock;
	}

	@Override
	public void updateOwnTime(int id) {
		//Not supported
	}

	@Override
	public void updateTimer(long[] otherVector) throws Exception {
		if(otherVector.length != 1)	throw new Exception("Time clock vector must have the same size: otherVector.length != _clock.length");
		_delay = otherVector[0] - (System.currentTimeMillis()+_delay);
	}

	@Override
	public void setAndUpdateTime(long[] otherVector, int id) throws Exception {
		updateTimer(otherVector);
	}

	@Override
	public void resetTimer() {
		_delay = (long)0;
	}
}
