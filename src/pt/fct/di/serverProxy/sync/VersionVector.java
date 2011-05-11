package pt.fct.di.serverProxy.sync;

import java.util.Map;

/**
 * This class represents a server version vector. 
 * A version vector is a list containing the timestamp of the last update operation executed successfully of all known clients.
 * With this strucuture it is possible know how out of sync a server is from another, and exactly know what operations are missing 
 * for prior synchronization.  
 * Here we will implement all methods necessary to update, remove, compare and manipule the values within.
 
 * @author andre_goncalves@di
 *
 */

//TODO: Verify if this synchronizes between get and update only one position is necessary. A primeira vista nao parece ser preciso porque nao
//afecta directamente o processo de sincronizacao entre servidores, mas apenas a rapidez com que os clientes veem as actualizacoes uns dos outros.
public class VersionVector {
	
	private final int DEFAULT_SIZE = 10;
	
	//Vector Object...
	private long[] _vector;
	
	public VersionVector()
	{
		_vector = new long[DEFAULT_SIZE];
	}
	
	public VersionVector(int size)
	{
		_vector = new long[size];
	}
	
	public void setNewVector(long[] otherVector)
	{
		_vector = otherVector;
	}
	
	public synchronized long[] getVector()
	{
		return _vector;
	}
	
	public void registerClient(int id)
	{
		_vector[id] = 0;
	}
	
	public void deleteClient(int id)
	{
		_vector[id] = 0;
	}
	
	public long setClientVersion(int id)
	{
		_vector[id] = _vector[id]+1;
		return _vector[id];
	}
	
	public long setClientVersion(int id, long value)
	{
		_vector[id] = _vector[id]+value;
		return _vector[id];
	}
	
	public synchronized long[] updateAndGetVector(int id)
	{
		_vector[id] = _vector[id]+1;
//		System.out.println("Clock: "+_vector[id]);
		return _vector;
	}
	
	public long getClientVersion(int id)
	{
		return _vector[id];
	}
	
	public boolean updated(long[] otherVector)
	{
		boolean updated = true;
		for(int i=0; i<_vector.length; i++)
		{
			if(_vector[i] < (otherVector[i]-1))
			{
				updated = false;
				break;
			}
		}
		return updated;
	}
	
	public boolean compare(long[] otherVector, Map<Integer,Long> missingUpdates)
	{
//		if(otherVector.length > _vector.length) _vector = Arrays.copyOf(_vector, otherVector.length);
		
		long diff = 0;
		for(int i=0; i < _vector.length; i++)
		{
			diff = otherVector[i] - _vector[i]; 
			if(diff > 1) missingUpdates.put(i, _vector[i]+1);
		}
		
		if(missingUpdates.size() > 0) return false;
		return true;
	}
}
