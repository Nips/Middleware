package pt.fct.di.clientProxy.service.fields.time;

public class LogicalTimer implements Timer{

	private long[] _clock;
	
	public LogicalTimer(long initialtime)
	{
		_clock = new long[1];
		_clock[0] = initialtime;
	}

	@Override
	public long[] getTimeVector() {
		return _clock;
	}

	@Override
	public void updateOwnTime(int id) {
		_clock[0] = _clock[0] + 1;
		
	}

	@Override
	public void updateTimer(long[] otherVector) throws Exception
	{
		if(otherVector.length != _clock.length)	throw new Exception("Time clock vector must have the same size: otherVector.length != _clock.length");
			_clock[0] = Math.max(_clock[0], otherVector[0]);
	}

	@Override
	public void setAndUpdateTime(long[] otherVector, int id) throws Exception{
		updateTimer(otherVector);
		updateOwnTime(id);
	}

	@Override
	public void resetTimer() {
		_clock = new long[1];
	}
}
