package pt.fct.di.serverProxy.sync;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.serverProxy.comm.internal.InternalTCPComm;

//import pt.fct.di.util.SystemProperties;

/**
 * This class controls and manages all version vector known to this server. 
 * Through this we can see if some vector out of date and issue a synchronization phase to transfer all missing operations.
 * Makes the bridge between communication system, version vector and service class.
 * @author andre_goncalves@di
 *
 */
public class SyncManager {

//	private final int DEFAULT_NUMBER_SERVERS = 3;
	
	private int _myId;
	private SyncThread _sync;
	private InternalTCPComm _comm;
	private long _waitingTime;
	
//	private long[] _
	private VersionVector[] _vectors;
	
	/**
	 * Properties for configuring this communication channel.
	 */
	private Properties _p=new Properties();
	
	public SyncManager(){ } 
	
	/**
	 * Set the properties for this communication channel.
	 */
	public void setProperties(Properties p)
	{
		_p=p;
	}
	
	public void init() throws SyncException
	{
		if(!_p.containsKey("serverId")) throw new SyncException("Field \"serverId\" not found. Please update conf file.");
		if(!_p.containsKey("syncWaitingTime")) throw new SyncException("Field \"syncWaitingTime\" not found. Please update conf file.");
		
		try {
			_comm = InternalTCPComm.getInstance();
			_myId = Integer.parseInt(_p.getProperty("serverId"));
			_vectors = new VersionVector[_comm.getNumberServers()];
			System.out.println("Number of Servers: "+_vectors.length);
			for(int pos = 0; pos<_vectors.length; pos++)
				_vectors[pos] = new VersionVector();
			_waitingTime = Long.parseLong(_p.getProperty("syncWaitingTime")); 
			if(_waitingTime > 500) initHandler();
		} catch (CommException e) {
			throw new SyncException(e);
		}
	}
	
	public void cleanup()
	{
		_sync.cleanup();
	}
	
	public long updateClientVersion(int client_id)
	{
		return _vectors[_myId].setClientVersion(client_id);
	}
	
	public long updateClientVersion(int client_id, long value)
	{
		return _vectors[_myId].setClientVersion(client_id, value);
	}
	
	public long[] updateAndGetVector(int client_id)
	{
		return _vectors[_myId].updateAndGetVector(client_id);
	}
	
	public void compareVersionVectors(long[] otherVector)
	{
		while(!_vectors[_myId].updated(otherVector))
			try {
				this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}
	
	public boolean verifyUpDate(long[] otherVector, Map<Integer,Long> missingUpdates)
	{
		return _vectors[_myId].compare(otherVector, missingUpdates);
	}
	
	public void updateVector(int pos, long[] otherVector)
	{
		assert pos >= 0 && pos < _vectors.length: "pos = "+pos;
		_vectors[pos].setNewVector(otherVector);
	}
	
	public long[] getOwnVector()
	{
		return _vectors[_myId].getVector();
	}
	
	public void initHandler()
	{
		_sync = new SyncThread(_waitingTime);
		_sync.start();
	}
	
	public void closeHandler()
	{
		_sync.cleanup();
		_sync.interrupt();
	}
	
	//TODO: New class that periodically sends this servers vector
	class SyncThread extends Thread
	{
		private long _waitingTime;
		private boolean _close;
		
		SyncThread(long time)
		{
			this._waitingTime = time;
			this._close = false;
		}
		
		public void setTime(long time)
		{
			_waitingTime = time;
		}
		
		public void run()
		{
			//TODO: spread the servers synchronization thread out so they don't all exchange version vectors at the same time
			Random random = new Random(System.currentTimeMillis());
			try {
				long aux = _waitingTime/60000; //60000 miliseconds = 1 minute (waiting more than a minute is too long for DB's to synchronize)
				
				//Throws exception if _aux>1 because random.nextInt argument must be >0
				//and the sleep() doesn't make sense for granularities < 1 ms anyway
				if((aux > 0) && (aux <= 1))
					sleep(random.nextInt((int)(1/aux)));

				//Main loop that periodically sends version vector to the other servers
				while(!_close)
				{
					sleep(_waitingTime);
					_comm.synchronize(_myId, getOwnVector());
				}	
			} catch (InterruptedException e) {
				System.out.println("SyncManager - run() thrown InterruptedException");
				cleanup();
			}
		}
		
		public void cleanup()
		{
			_close = true;
		}
	}
}
