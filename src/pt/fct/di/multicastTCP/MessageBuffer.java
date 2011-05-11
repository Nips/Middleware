package pt.fct.di.multicastTCP;

import java.util.concurrent.LinkedBlockingQueue;

//import pt.fct.di.serverProxy.comm.IServerComm.Message;
import pt.fct.di.ops.Operation;
import pt.fct.di.util.Constants;

public class MessageBuffer {

	private static MessageBuffer buffer = null;
	
	private LinkedBlockingQueue<Operation> _opsQueue;
	private int _size;
	
	private MessageBuffer(int size)
	{
		_size = size;
	}
	
	public static MessageBuffer getInstance(int size)
	{
		if(buffer == null){
			if(size < 1) buffer = new MessageBuffer(Integer.parseInt(Constants.DEFAULT_MESSAGE_BUFFER_SIZE));
			else buffer = new MessageBuffer(size);
			buffer.init();
		}
		return buffer;
	}
	
	private void init() 
	{
		_opsQueue = new LinkedBlockingQueue<Operation>(_size);
	}
	
	public void insertMsg(Operation e) throws InterruptedException
	{
		_opsQueue.put(e);
	}
	
	public Operation removeMsg() throws InterruptedException
	{
		return _opsQueue.take();
	}
	
}
