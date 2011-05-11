package pt.fct.di.serverProxy.service;

import java.util.concurrent.LinkedBlockingQueue;

//import pt.fct.di.serverProxy.comm.IServerComm.Message;
import pt.fct.di.util.Constants;

public class MessageBuffer {

//	private static MessageBuffer buffer = null;
//	
//	private LinkedBlockingQueue<Message> _opsQueue;
//	private int _size;
//	
//	private MessageBuffer(int size)
//	{
//		_size = size;
//	}
//	
//	public static MessageBuffer getInstance()
//	{
//		if(buffer == null){
//			buffer = new MessageBuffer(Integer.parseInt(Constants.DEFAULT_MESSAGE_BUFFER_SIZE));
//			buffer.init();
//		}
//		return buffer;
//	}
//	
//	private void init() 
//	{
//		_opsQueue = new LinkedBlockingQueue<Message>(_size);
//	}
//	
//	public void insertMsg(Message e) throws InterruptedException
//	{
//		_opsQueue.put(e);
//	}
//	
//	public Message removeMsg() throws InterruptedException
//	{
//		return _opsQueue.take();
//	}
	
}
