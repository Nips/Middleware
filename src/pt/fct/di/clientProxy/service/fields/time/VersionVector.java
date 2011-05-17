package pt.fct.di.clientProxy.service.fields.time;

import java.util.concurrent.atomic.AtomicInteger;

public class VersionVector implements Timer{
	
	private static final int DEFAULT_VECTOR_SIZE = 10;
	
	private long[] _versionVector;
	private int _size;
	
	private AtomicInteger nUpdates = new AtomicInteger(0);
	
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
		return _versionVector.clone();
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
//		nUpdates.incrementAndGet();
//		printVector();
		if((nUpdates.incrementAndGet() % 1000)==0) printVector();
	}

	public void setAndUpdateTime(long[] otherVector, int id) throws Exception
	{
//		updateTimer(otherVector);
//		updateOwnTime(id);
	}

	public void resetTimer()
	{
		_versionVector = new long[_size];
	}
	
	private void printVector()
	{
		System.out.print("VersionVector "+nUpdates.get()+": [ ");
		for(int pos = 0; pos < _versionVector.length; pos++)
			System.out.print(_versionVector[pos]+", ");
		System.out.println("]");
	}
}
