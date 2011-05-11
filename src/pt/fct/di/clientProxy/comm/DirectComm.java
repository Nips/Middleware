package pt.fct.di.clientProxy.comm;

import pt.fct.di.client.CException;
import pt.fct.di.ops.OpException;
import pt.fct.di.ops.Operation;
import pt.fct.di.ops.ReadResult;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.serverProxy.comm.ServerDirectComm;

public class DirectComm extends ClientComm {

	ServerDirectComm _server;
	
	public DirectComm()
	{
		
	}
	
	public void init() throws CException
	{
		_server = new ServerDirectComm();
		_server.setProperties(getProperties());
		try {
			_server.init();
		} catch (CommException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void cleanup()
	{
		_server.cleanup();
	}
	
	public void sendMessage(Operation op) throws CException
	{
		try{
			ReadResult result = _server.receive(op.toByteArray());
			if( isOpRegistered(result.getID()) )
				addResultToQueue(result);
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (OpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ReadResult sendMessageReceive(Operation op) throws CException
	{
		/*try {
			if(syncOp(op.getID()))
			{
				sendMessage(op);
				return removeResultFromQueue();
			}
			else
				return new Result("error", op.getID(), -1, 
						"Cannot sync operation", op.getTS());
		} catch (InterruptedException e) {
			CException ce = new CException("sendMessageReceive - No element was found in responses queue");
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}*/
		
		try {
			return _server.receive(op.toByteArray());
		} catch (OpException e) {
			CException ce = new CException(e.getMessage());
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
	}
}
