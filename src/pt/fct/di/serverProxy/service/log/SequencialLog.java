package pt.fct.di.serverProxy.service.log;

import java.util.Collection;
import java.util.Iterator;
import java.util.PriorityQueue;

import pt.fct.di.ops.log.ILogOperation;

public class SequencialLog {

	private final int INITIAL_CAPACITY = 5000;
	
	private PriorityQueue<ILogOperation> _sequencialLog;
	
	public SequencialLog()
	{
		_sequencialLog = new PriorityQueue<ILogOperation>();
	}
	
	public SequencialLog(long waitingTime)
	{
		long syncPoint = (waitingTime*2)/1000; //Convert from miliseconds to seconds
		_sequencialLog = new PriorityQueue<ILogOperation>((int)(INITIAL_CAPACITY*syncPoint)); //Assuming that your throughput is about 5000 ops/s this queue must have enough space to store
																							  //all operations until next synchronization point. (this avoid unnecessary growing of the queue)
	}
	
	public SequencialLog(Collection<ILogOperation> collection)
	{
		_sequencialLog = new PriorityQueue<ILogOperation>(collection);
	}
	
	public synchronized boolean put(ILogOperation op)
	{
		boolean set = _sequencialLog.add(op); 
//		System.out.println("Is op inside log? "+_sequencialLog.contains(op));
		return set;
	}
	
	public void deleteOldOperations(long endTimestamp)
	{
		ILogOperation op = null;
		
		while(!_sequencialLog.isEmpty())
		{
			op = _sequencialLog.peek();
			if(op.getTS() <= endTimestamp)
			{
//				System.out.println("Removing...");
				_sequencialLog.remove();
			}
			else break;
		}
	}
	
	public String toString()
	{
		String msg = "		======================		\n";
		msg += 		 "		||  Sequencial Log  ||		\n";
		msg += 		 "		======================		\n";
		Iterator<ILogOperation> it = _sequencialLog.iterator();
		msg += "{ ";
		while(it.hasNext())
		{
			ILogOperation op = it.next();
			msg += op.toString();
			msg += ", ";
		}
		msg += "}\n";
		msg += "******************************************";
		msg += "";
		return msg;
	}
}
