package pt.fct.di.multicastTCP;

import pt.fct.di.clientProxy.comm.ClientComm;
import pt.fct.di.clientProxy.concurrent.OperationQueue;
import pt.fct.di.clientProxy.net.IAsyncCallback;
import pt.fct.di.clientProxy.service.ProxyService;
import pt.fct.di.ops.IClientOperation;
import pt.fct.di.ops.IResult;
//import pt.fct.di.ops.Operation;
//import pt.fct.di.ops.ReadResult;
import pt.fct.di.util.SystemProperties;

public class TransportThread extends Thread{

//	private final int OPERATIONS_IN_BUFFER = 40;
	
	private TSocket _socket = null;
	private boolean _close = false;
	private boolean _debug = false;
	
	private OperationQueue _opQueue = null;
	
	public TransportThread(TSocket t)
	{
		_socket = t;
//		_opQueue = new OperationQueue(OPERATIONS_IN_BUFFER);
		_opQueue = new OperationQueue();
		_debug = SystemProperties._debug;
	}
	
	public void init() throws TCPException
	{
		if(!_socket.isOpen()) _socket.open();
		_opQueue.init();
		this.start();
	}
	
	public void close()
	{
//		System.out.println("Closing TransportThread!!");
		_close = true;
		_opQueue.close();
		_socket.close();
		this.interrupt();
	}
	
	public TSocket getSocket()
	{
		return _socket;
	}
	
	public void sendMessage(IClientOperation op) throws TransportException
	{
//		if(!_socket.isOpen()) throw new TransportException("Socket not connected");
		try {
			_opQueue.insertMsg(op);
//			_socket.sendMessage(op);
//		} catch (TCPException e) {
//			throw new TransportException(e);
		} catch (InterruptedException e) {
			throw new TransportException(e);
		}
	}
	
//	public void sendMessage(Operation op) throws TransportException
//	{
//		try {
//			_opQueue.insertMsg(op);
//		} catch (InterruptedException e) {
//			throw new TransportException(e);
//		}
//	}
	
	public void run()
	{
		IResult r = null;
		IAsyncCallback cb = null;
		while(!_close /*|| !_opQueue.isEmpty()*/)
		{
			try {
				//TREAT RECEIVE RESULTS 
//				System.out.println(_close);
				_socket.sendMessage(_opQueue.removeMsg());
				
				//Waits until a result arrive
				r = _socket.receiveMessage();
				
				try {
					ProxyService.updateTimeVector(r.getVersionVector());
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
				
//				System.out.println(r.toString());
//				System.out.println("Result size: "+r.serialize().length);
				//System.out.println("Received result: \n"+r.toString());
				
				//Get and put message in ASyncCallback
				cb = ClientComm.getRegisteredCallback(r.getOpSeq());			
				if(cb!=null) cb.response(r);
				//else ignore message and carry on...
			} catch (TCPException e) {
				if(_debug) e.printStackTrace();
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
//				if(_debug) e.printStackTrace();
//				throw new RuntimeException(e);
			}
		}
//		System.out.println("TERMINOU!!!!");
	}
}
