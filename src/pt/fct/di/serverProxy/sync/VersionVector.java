package pt.fct.di.serverProxy.sync;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
	private AtomicLong[] _vector;
//	private long[] _vector;
	
	private AtomicInteger nUpdates = new AtomicInteger(0); //For testing porposes
	
	public VersionVector()
	{
//		_vector = new long[DEFAULT_SIZE];
		_vector = new AtomicLong[DEFAULT_SIZE];
		for(int pos = 0; pos < DEFAULT_SIZE; pos++) _vector[pos] = new AtomicLong(0);
	}
	
	public VersionVector(int size)
	{
//		_vector = new long[size];
		_vector = new AtomicLong[size];
		for(int pos = 0; pos < size; pos++) _vector[pos] = new AtomicLong(0);
	}
	
	//TODO: Is this useful? Maybe to update the vectors received by other servers
	public void setNewVector(long[] otherVector)
	{
//		_vector = otherVector;
		AtomicLong[] aux = new AtomicLong[otherVector.length];
		for(int pos=0; pos < otherVector.length; pos++)
			aux[pos] = new AtomicLong(otherVector[pos]);
		_vector = aux;
	}
	
	public long[] getVector()
	{
//		long[] clone = _vector.clone();
//		return clone;
		long[] aux = new long[_vector.length];
		for(int pos=0; pos < aux.length; pos++ ) aux[pos] = _vector[pos].get();
		return aux;
	}
	
//	public void registerClient(int id)
//	{
//		_vector[id] = new AtomicLong(0);
//	}
	
//	public void deleteClient(int id)
//	{
//		_vector[id] = 0;
//	}
	
	public long getClientVersion(int id)
	{
//		return _vector[id];
		return _vector[id].get();
	}
	
	//TODO: Is this useful?
	public long setClientVersion(int id)
	{
//		_vector[id] = _vector[id] + 1;
//		return _vector[id];
//		return _vector[id].incrementAndGet();
		return -1;
	}
	
	public long setClientVersion(int id, long value)
	{
//		_vector[id] = _vector[id] + value;
//		return _vector[id];
		return _vector[id].addAndGet(value);
	}
	
	public long[] updateAndGetVector(int id, long ts)
	{
//		System.out.println("CurrentValue: "+_vector[id]+", OtherTs: "+ts); //testing mode!
		
//		_vector[id] = _vector[id] + 1;
//		_vector[id] = Math.max(_vector[id], ts) + 1; //wrong way to do things. Each servers must update its own version vector at his speed
//		System.out.println("Result: "+_vector[id]); //testing mode!
//		if((nUpdates.incrementAndGet() % 1000)==0) printVector(); testing mode!
//		return _vector.clone();
		
		_vector[id].incrementAndGet();
		long[] aux = new long[_vector.length];
		for(int pos=0; pos < aux.length; pos++ ) aux[pos] = _vector[pos].get();
		return aux;
	}
	
	
	//TODO: Transform this method to a more generic one
	public boolean updated(long[] otherVector)
	{
		long[] aux = getVector();
		boolean updated = true;
		for(int i=0; i<_vector.length; i++)
		{
			if(aux[i] < (otherVector[i]-1))
			{
				updated = false;
				break;
			}
		}
		return updated;
	}
	
	//TODO: Verify the validity of this method
	public boolean compare(long[] otherVector, Map<Integer,Long> missingUpdates)
	{
//		if(otherVector.length > _vector.length) _vector = Arrays.copyOf(_vector, otherVector.length);
		
		long[] aux = getVector();
		long diff = 0;
		for(int i=0; i < _vector.length; i++)
		{
			long value = aux[i];
			diff = otherVector[i] - value; 
			if(diff > 1) missingUpdates.put(i, value+1);
		}
		
		if(missingUpdates.size() > 0) return false;
		return true;
	}
	
	private void printVector()
	{
		System.out.print("VersionVector "+nUpdates.get()+": [ ");
		for(int pos = 0; pos < _vector.length; pos++)
			System.out.print(_vector[pos]+", ");
		System.out.println("]");
	}
}
