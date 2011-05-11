package pt.fct.di.clientProxy.comm;

import java.util.HashMap;
import java.util.Vector;

import pt.fct.di.client.CException;
import pt.fct.di.ops.Operation;
import pt.fct.di.ops.ReadResult;

public class DummyComm extends ClientComm{
	
	public void init() throws CException
	{
		return;
	}
	
	public void cleanup()
	{
		return;
	}
	
	public void sendMessage(Operation op) throws CException
	{
		new Insert(op).start();
	}
	
	private ReadResult getResultbyType(Operation op)
	{
		String type = op.getType();
		if(type.equals("read")){
			return new ReadResult(type, op.getID(), 0, new HashMap<String, String>(), System.nanoTime());
		}
		else if(type.equals("scan")){
			return new ReadResult(type, op.getID(), 0, new Vector<HashMap<String, String>>(), System.nanoTime());
		}
		else if(type.equals("insert")){
			return new ReadResult(type, op.getID(), 0, "", System.nanoTime());
		}
		else if(type.equals("update")){
			return new ReadResult(type, op.getID(), 0, "", System.nanoTime());
		}
		else if(type.equals("delete")){
			return new ReadResult(type, op.getID(), 0, "", System.nanoTime());
		}
		else
		{
			System.err.println("OP Not Recognized");
			return new ReadResult("error", op.getID(), -1, "Op not Recognized", op.getTS());
		}
	}
	
	public ReadResult sendMessageReceive(Operation op) throws CException
	{
		try {
			if(syncOp(op.getID()))
			{
				sendMessage(op);
				return removeResultFromQueue(); 
			}
			else
				return new ReadResult("error", op.getID(), -1, 
						"Cannot sync operation", op.getTS());
		} catch (InterruptedException e) {
			CException ce = new CException("sendMessageReceive - No element was found in responses queue");
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
	}
	
	class Insert extends Thread
	{
		Operation _op;
		
		public Insert(Operation op)
		{
			this._op = op;
		}
		
		public void run()
		{
			try {
				ReadResult result = getResultbyType(_op);
				if( isOpRegistered(result.getID()) )
					addResultToQueue(result);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
