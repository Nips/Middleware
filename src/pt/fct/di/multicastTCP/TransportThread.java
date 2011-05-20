package pt.fct.di.multicastTCP;

import java.util.concurrent.atomic.AtomicInteger;

import pt.fct.di.client.CException;
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
	
	public TransportThread(TSocket t, boolean debug)
	{
		_socket = t;
//		_opQueue = new OperationQueue(OPERATIONS_IN_BUFFER);
		_opQueue = new OperationQueue();
		_debug = debug;
	}
	
	public void init() throws TCPException
	{
		if(!_socket.isOpen()) _socket.open();
		_opQueue.init();
		this.start();
	}
	
	public void close()
	{
		System.out.println("Operations left in queue for server "+_socket.getRemoteAddress()+": "+_opQueue.getSize());
//		System.out.println("Closing TransportThread!!");
		_close = true;
		_opQueue.close();
//		_socket.close();
//		this.interrupt();
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
		ProxyService service;
		try {
			service = ProxyService.getInstance();
		} catch (CException e1) {
			System.out.println("CException detected in TransportThread");
			return;
		}
		
		IClientOperation op = null;
		IResult r = null;
		IAsyncCallback cb = null;
		while(!_close /*|| !_opQueue.isEmpty()*/)
		{
			try {
				//TREAT RECEIVE RESULTS 
//				System.out.println(_close);
				
				op = _opQueue.removeMsg();
//				if(_debug) System.out.println(op.toString());
//				{
//					System.out.print("Op to send - opseq: "+op.getOpSeq()+", id: "+op.getID()+", vector [");
//					long[] vector = op.getVersionVector();
//					for(int pos = 0; pos < vector.length; pos++)
//					System.out.print(vector[pos]+", ");
//					System.out.println("]");
//				}
				
				_socket.sendMessage(op);
				
				//Waits until a result arrive
				r = _socket.receiveMessage();
				
//				if(_debug)
//				{
//					System.out.print("Receiving: Code "+r.getCode()+" Vector [");
//					long[] vector = r.getVersionVector();
//					for(int pos = 0; pos < vector.length; pos++)
//						System.out.print(vector[pos]+", ");
//					System.out.println("]");
//				}
				
				
				//TODO: Change this after testing to r.getVersionVector()
				try {
					long[] vv = r.getVersionVector();
					service.updateTimeVector(vv);
//					if(r.getCode() > 0)
//					{
//						long[] aux = new long[10];
//						aux[op.getID()] = time++;
//						service.updateTimeVector(aux);
//					}
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
				
				//Get and put message in ASyncCallback
				cb = ClientComm.getRegisteredCallback(r.getOpSeq());			
				if(cb!=null) cb.response(r);
				//else ignore message and carry on...
			} catch (TCPException e) {
				//if(_debug) e.printStackTrace();
//				throw new RuntimeException(e);
				System.out.println("TCPException detected in method run in TransportThread");
				return; //This exception states that the connection establish with server broke down. ReEstablish connection or Give up?
			} catch (InterruptedException e) {
//				if(_debug) e.printStackTrace();
//				throw new RuntimeException(e);
				System.out.println("InterruptedException detected in method run in TransportThread");
				return;
			}
		}
//		System.out.println("TERMINOU!!!!");
	}
}
