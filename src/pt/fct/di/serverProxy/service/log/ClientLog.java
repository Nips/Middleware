package pt.fct.di.serverProxy.service.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import pt.fct.di.ops.log.ILogOperation;

public class ClientLog {
	private Map<Integer,TreeSet<ILogOperation>> _clientLog;
	
	public ClientLog()
	{
		_clientLog = new HashMap<Integer,TreeSet<ILogOperation>>();
	}
	
	public ClientLog(int initialSize)
	{
		_clientLog = new HashMap<Integer,TreeSet<ILogOperation>>(initialSize);
	}
	
	public void put(Integer clientId, ILogOperation op)
	{
		TreeSet<ILogOperation> pq = _clientLog.get(clientId); 
		if(pq == null)
		{
			pq = new TreeSet<ILogOperation>();
			pq.add(op);
			_clientLog.put(clientId, pq);
		}
		else pq.add(op);
//		System.out.println("Is op inside log? "+pq.contains(op));
	}
	
	public Set<Map.Entry<Integer,TreeSet<ILogOperation>>> entrySet() {
		return _clientLog.entrySet();
	}
	
	public List<ILogOperation> getOperations(Integer clientId, Long startTimestamp)
	{
		TreeSet<ILogOperation> logOps = _clientLog.get(clientId);
		List<ILogOperation> syncList = new ArrayList<ILogOperation>(logOps.size());
		for(ILogOperation op : logOps)
			if(op.getTS() >= startTimestamp) syncList.add(op);
		return syncList;
	}
	
	public void deleteOldOperations(long endTimestamp) //Change to boolean if necessary (in case i need to know if anything was changed)
	{
		TreeSet<ILogOperation> queue = null;
		ILogOperation nextOperation = null;
		for(Map.Entry<Integer, TreeSet<ILogOperation>> entry: entrySet())
		{	
			queue = entry.getValue();
			while(!queue.isEmpty())
			{
				nextOperation = queue.first();
				if(nextOperation.getTS() <= endTimestamp)
				{
//					System.out.println("Removing Op from ClientLog...");
					queue.pollFirst();
				}
				else break;
			}
			if(queue.isEmpty()) _clientLog.remove(entry.getKey());
		}
	}
	
	public String toString()
	{
		String msg = "		======================		\n";
		msg += 		 "		||    Client Log    ||		\n";
		msg += 		 "		======================		\n";
		Iterator<Map.Entry<Integer, TreeSet<ILogOperation>>> it = _clientLog.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<Integer, TreeSet<ILogOperation>> entry = it.next();
			msg += "Client Id => ";
			ILogOperation next = null;
			msg += "{ ";
			Iterator<ILogOperation> log = entry.getValue().iterator();
			while(log.hasNext())
			{
				next = log.next();
				msg+= " "+next.toString()+", ";
			}
			msg += "}\n";
			msg += "******************************************";
			msg += "\n";
		}
		return msg;
	}
}
