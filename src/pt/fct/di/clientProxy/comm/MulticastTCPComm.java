package pt.fct.di.clientProxy.comm;

import java.net.UnknownHostException;

import pt.fct.di.clientProxy.net.IAsyncCallback;
import pt.fct.di.multicastTCP.MSocket;
import pt.fct.di.ops.IClientOperation;
//import pt.fct.di.ops.Operation;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.util.Constants;
//import pt.fct.di.util.SystemProperties;

public class MulticastTCPComm extends ClientComm{

	private MSocket _socket = null;
	private String[] _remoteAddrs = null;
	private String _addr;
	private int _connectionRetries;
//	private boolean _debug;
	
	@Override
	public void init() throws CommException {
		String hosts = getProperties().getProperty("hosts");
		if(hosts==null) throw new CommException("Required property \"hosts\" missing for MulticasTCPComm");
		
		_connectionRetries = Integer.parseInt(getProperties().getProperty("connectionRetries", Constants.DEFAULT_CONNECTION_RETRY_PROPERTY));
//		_debug = SystemProperties._debug;
		
		String[] allhosts=hosts.split(",");
		System.out.println(allhosts.length);
		_remoteAddrs = allhosts;
		
		Exception connectexception = null;
		
		for(int retry=0; retry<_connectionRetries; retry++)
		{
			try
			{
				_socket = new MSocket(allhosts);
				_socket.open();
				_addr = _socket.getLocalAddr();
				break;
			}
			catch(Exception e)
			{ 
				connectexception = e; 
			}
			try
			{
				Thread.sleep(1000);
			}
			catch(InterruptedException e){ }
		}
		if(connectexception!=null)
		{
			System.err.println("Unable to connect due to an exception:");
			connectexception.printStackTrace();
			throw new CommException(connectexception);
		}
	}

	@Override
	public void cleanup() {
//		System.out.println("CallbackMap #elems: "+_callbackMap.size());
		_socket.close();
	}

	@Override
	public String getAddr() throws UnknownHostException {
//		if(_debug) System.out.println("My local address is: "+_addr);
		return _addr;
	}
	
	@Override
	public String getReaderAddr(){
		return _remoteAddrs[0];
	}
	
	@Override
	public int getNumberServers()
	{
		return _remoteAddrs.length;
	}
	
	@Override
	public void sendOneWay(IClientOperation op, String addr) throws CommException
	{
		op.setVersionVector(_service.getTimeVector());
//		if( _debug ) System.out.println(op.toString());
		try
		{
//			op.opValidation();
			//System.out.println(op.getType()+" size: "+op.serialize().length);
			_socket.sendMessage(op, addr);
//			if(_debug) System.out.println("Message sended...");
		}
		catch(Exception e){ throw new CommException(e); }
	}

	@Override
	public void sendOneWay(IClientOperation op) throws CommException {
		op.setVersionVector(_service.getTimeVector());
//		if( _debug ) System.out.println(op.toString());
		try
		{
//			op.opValidation();
			//System.out.println(op.getType()+" size: "+op.serialize().length);
			_socket.sendMessage(op);
//			if(_debug) System.out.println("Message sended...");
		}
		catch(Exception e){ throw new CommException(e); }
	}

	@Override
	public int sendMessage(IClientOperation op, IAsyncCallback cb)
			throws CommException {
		int seqNumber = op.getOpSeq();
//		if(_debug) System.out.println(id);
		ClientComm.addCallback(cb, seqNumber);
//		op.setID(_service.getId());
//		op.setOpSeq(seqNumber);
		sendOneWay(op);
		return seqNumber;
	}
	
	@Override
	public int sendReadMessage(IClientOperation op, IAsyncCallback cb)
			throws CommException {
		int seqNumber = op.getOpSeq();
//		if(_debug) System.out.println(id);
		ClientComm.addCallback(cb, seqNumber);
//		op.setID(_service.getId());
//		op.setOpSeq(seqNumber);
		sendOneWay(op, getReaderAddr());
		return seqNumber;
	}
}