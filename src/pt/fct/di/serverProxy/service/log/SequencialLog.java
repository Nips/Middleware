package pt.fct.di.serverProxy.service.log;

import java.util.Collection;
//import java.util.Iterator;
//import java.util.PriorityQueue;
import java.util.TreeSet;

import pt.fct.di.ops.log.ILogOperation;

public class SequencialLog {

//	private final int INITIAL_CAPACITY = 5000;
	
//	private PriorityQueue<ILogOperation> _sequencialLog;
	private TreeSet<ILogOperation> _sequencialLog;
	
	public SequencialLog()
	{
//		_sequencialLog = new PriorityQueue<ILogOperation>();
		_sequencialLog = new TreeSet<ILogOperation>();
	}
	
//	public SequencialLog(long waitingTime)
//	{
//		long syncPoint = (waitingTime*2)/1000; //Convert from miliseconds to seconds
//		_sequencialLog = new PriorityQueue<ILogOperation>((int)(INITIAL_CAPACITY*syncPoint)); //Assuming that your throughput is about 5000 ops/s this queue must have enough space to store
//																							  //all operations until next synchronization point. (this avoid unnecessary growing of the queue)
//	}
	
	public SequencialLog(Collection<ILogOperation> collection)
	{
//		_sequencialLog = new PriorityQueue<ILogOperation>(collection);
		_sequencialLog = new TreeSet<ILogOperation>(collection);
	}
	
	public synchronized int getSize()
	{
		return _sequencialLog.size();
	}
	
	public synchronized boolean put(ILogOperation op)
	{
//		boolean set = _sequencialLog.offer(op);
		boolean set = _sequencialLog.add(op);
//		System.out.println("Is op inside log? "+_sequencialLog.contains(op));
		return set;
	}
	
	//TODO: Verify this method for correctness!!!!!
	public synchronized void deleteOldOperations(long endTimestamp)
	{
		ILogOperation op = null;
		
		while(!_sequencialLog.isEmpty())
		{
//			op = _sequencialLog.peek();
			op = _sequencialLog.first();
			if(op.getTS() <= endTimestamp)
			{
//				System.out.println("Removing...");
				_sequencialLog.remove(op);
			}
			else break;
		}
	}
	
	public String toString()
	{
//		PriorityQueue<ILogOperation> aux = new PriorityQueue<ILogOperation>(_sequencialLog.size());
		String msg = "		======================		\n";
		msg += 		 "		||  Sequencial Log  ||		\n";
		msg += 		 "		======================		\n";
//		Iterator<ILogOperation> it = _sequencialLog.iterator();
		msg += "{ ";
//		while(!_sequencialLog.isEmpty())
//		{
//			ILogOperation op = _sequencialLog.poll();
//			msg += op.toString();
//			msg += ", \n";
//			aux.offer(op);
//		}
//		_sequencialLog = aux;
		for(ILogOperation op : _sequencialLog)
		{
			msg += op.toString();
			msg += ", \n";
		}
		msg += "}\n";
		msg += "******************************************";
		msg += "";
		return msg;
	}
}
