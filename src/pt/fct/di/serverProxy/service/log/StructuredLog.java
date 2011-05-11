package pt.fct.di.serverProxy.service.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.util.Pair;

public class StructuredLog {
	private Map<Pair<String, String>, LinkedList<ILogOperation>> _structuredLog;
	
	public StructuredLog()
	{ 
		_structuredLog = new HashMap<Pair<String,String>,LinkedList<ILogOperation>>();
	}
	
	public StructuredLog(int initialSize)
	{ 
		_structuredLog = new HashMap<Pair<String,String>,LinkedList<ILogOperation>>(initialSize);
	}
	
	public LinkedList<ILogOperation> get(Pair<String,String> familyAndKey)
	{
		return _structuredLog.get(familyAndKey);
	}
	
	public boolean containsKey(Pair<String,String> familyAndKey)
	{
		return _structuredLog.containsKey(familyAndKey);
	}

	public void put(Pair<String, String> familyAndKey,
			LinkedList<ILogOperation> newRecord) {
		_structuredLog.put(familyAndKey, newRecord);	
	}
	
	public ListIterator<ILogOperation> putInRecord(LinkedList<ILogOperation> record, ILogOperation op)
	{	
		int compare = 0;
		
		ListIterator<ILogOperation> it = record.listIterator(record.size());
		//System.out.println(it.hasPrevious()+" "+it.hasNext());
		
		//interate list backwards until we find the correct position for op
		while(it.hasPrevious() && (compare=op.compareTo(it.previous())) < 0){ }
		if(it.hasPrevious() || compare > 0) it.next(); // just to correct the index position to insert op correctly in the log only if it is to insert on position > 1 && position <= size;
		it.add(op);
		return it;
	}
	
	public void putInRecord(ILogOperation op)
	{
		int compare = 0;
		
		List<ILogOperation> record = _structuredLog.get(op.getFamilyAndKey());
		ListIterator<ILogOperation> it = record.listIterator(record.size());
		//System.out.println(it.hasPrevious()+" "+it.hasNext());
		while(it.hasPrevious() && (compare=op.compareTo(it.previous())) < 0){ } //interate list backwards until we find the correct position for op
		if(it.hasPrevious() || compare > 0) it.next(); // just to correct the index position to insert op correctly in the log;
		it.add(op);
//		System.out.println("Is op inside log? "+(it.previous().compareTo(op)==0));
	}

	public Set<Map.Entry<Pair<String,String>,LinkedList<ILogOperation>>> entrySet() {
		return _structuredLog.entrySet();
	}
	
	public void deleteOldOperations(long endTimestamp) //Change to boolean if necessary (in case i need to know if anything was changed)
	{
		Iterator<ILogOperation> it = null;
		List<Pair<String,String>> keysToDelete = new ArrayList<Pair<String,String>>(_structuredLog.size());
		
		for(Map.Entry<Pair<String,String>, LinkedList<ILogOperation>> entry: entrySet())
		{
			if(entry.getValue().getLast().getTS() <= endTimestamp) keysToDelete.add(entry.getKey());
			else
			{
				it = entry.getValue().iterator();
				while(it.hasNext())
				{
					if(it.next().getTS() <= endTimestamp) it.remove();
				}
				if(entry.getValue().isEmpty()) keysToDelete.add(entry.getKey());
			}
		}
		for(Pair<String,String> key : keysToDelete)
		{
			_structuredLog.remove(key);
		}
	}
	
	public String toString()
	{
		String msg = "		======================		\n";
		msg += 		"		||  Structured Log  ||		\n";
		msg += 		"		======================		\n";
		Iterator<Entry<Pair<String,String>, LinkedList<ILogOperation>>> it = _structuredLog.entrySet().iterator();
		while(it.hasNext())
		{
			Entry<Pair<String, String>, LinkedList<ILogOperation>> entry = it.next();
			msg += "Family: "+entry.getKey().get_value1()+" and row key: "+entry.getKey().get_value2()+" =>  ";
			ILogOperation next = null;
			msg += " {";
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
