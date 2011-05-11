package pt.fct.di.clientProxy.service.fields;

import java.util.concurrent.atomic.AtomicInteger;

public class SequencerId {

	private final AtomicInteger _seqId = new AtomicInteger(0);
	private static SequencerId _instance = null;
	
	public static synchronized SequencerId getInstance()
	{
		if(_instance == null){ _instance = new SequencerId(); return _instance; }
		return _instance;
	}	
	
	public int getNextId()
	{
		return _seqId.incrementAndGet();
	}
	
}
