package pt.fct.di.clientProxy.concurrent;

import java.util.concurrent.LinkedBlockingQueue;

//import pt.fct.di.serverProxy.comm.IServerComm.Message;
import pt.fct.di.ops.IClientOperation;
//import pt.fct.di.ops.Operation;
//import pt.fct.di.util.Constants;

public class OperationQueue {
	
	private LinkedBlockingQueue<IClientOperation> _opsQueue;
//	private int _size;
	
	public OperationQueue(){ }
	
//	public OperationQueue(int size)
//	{
////		_size = size;
////		_size = 0;
//	}
	
//	public static MessageBuffer getInstance()
//	{
//		if(buffer == null){
//			buffer = new MessageBuffer(Integer.parseInt(Constants.DEFAULT_MESSAGE_BUFFER_SIZE));
//			buffer.init();
//		}
//		return buffer;
//	}
	
	public void init() 
	{
//		_opsQueue = new LinkedBlockingQueue<Operation>(_size);
		_opsQueue = new LinkedBlockingQueue<IClientOperation>();
	}
	
	public void close()
	{
	//	System.out.println("Max Size of queue: "+_size);
		_opsQueue = null;
	}
	
	public void insertMsg(IClientOperation e) throws InterruptedException
	{
//		if(_opsQueue.size() == _size) System.out.println("Queue is full");
		_opsQueue.put(e);
//		_size = Math.max(_size, _opsQueue.size());
	}
	
	public IClientOperation removeMsg() throws InterruptedException
	{
//		Operation op = _opsQueue.take();
//		System.out.print(_opsQueue.size()+" ");
		return _opsQueue.take();
	}
	
	public boolean isEmpty()
	{
		return this._opsQueue.isEmpty();
	}
	
	public int getSize()
	{
		return _opsQueue.size();
	}
	
}
