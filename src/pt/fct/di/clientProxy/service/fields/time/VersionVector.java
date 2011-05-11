package pt.fct.di.clientProxy.service.fields.time;

public class VersionVector implements Timer{
	
	private static final int DEFAULT_VECTOR_SIZE = 10;

	private long[] _versionVector;
	private int _size;
	
	public VersionVector()
	{
		_versionVector = new long[DEFAULT_VECTOR_SIZE];
		_size = DEFAULT_VECTOR_SIZE;
	}
	
	public VersionVector(int size)
	{
		_versionVector = new long[size];
		_size = size;
	}
	
	public synchronized long[] getTimeVector()
	{
		return _versionVector;
	}
	
	public void updateOwnTime(int id)
	{
		_versionVector[id] = _versionVector[id] + 1;
	}

	public synchronized void updateTimer(long[] otherVector) throws Exception
	{
//		if(otherVector.length != _versionVector.length)	throw new Exception("Time clock vector must have the same size: otherVector.length != _clock.length");
		for(int pos=0; pos<_versionVector.length; pos++)
			_versionVector[pos] = Math.max(_versionVector[pos], otherVector[pos]);
	}

	public void setAndUpdateTime(long[] otherVector, int id) throws Exception
	{
		updateTimer(otherVector);
		updateOwnTime(id);
	}

	public void resetTimer()
	{
		_versionVector = new long[_size];
	}
}
