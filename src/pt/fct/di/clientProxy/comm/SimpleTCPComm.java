package pt.fct.di.clientProxy.comm;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import pt.fct.di.client.CException;
import pt.fct.di.clientProxy.net.IAsyncCallback;
import pt.fct.di.clientProxy.service.ProxyService;
import pt.fct.di.multicastTCP.TCPException;
import pt.fct.di.ops.IClientOperation;
import pt.fct.di.ops.IResult;
//import pt.fct.di.ops.OpException;
//import pt.fct.di.ops.Operation;
import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.serverProxy.comm.CommException;
//import pt.fct.di.util.Constants;
import pt.fct.di.util.SystemProperties;
import pt.fct.di.util.Utils;

public class SimpleTCPComm extends ClientComm{
	
	private Socket _socket;
	private ObjectOutputStream _oos;
	protected ObjectInputStream _ois;
	private String _addr;
	
	private boolean _debug;
	
	private ReceiverThread _receiver = null;

	@Override
	public void init() throws CommException
	{	
		String hosts = getProperties().getProperty("hosts");
		if(hosts==null) throw new CommException("Required property \"hosts\" missing for MulticasTCPComm");
		
		String[] allhosts=hosts.split(",");
		if(allhosts.length > 1) throw new CommException("SimpleTCPComm - hosts property with more than one address: allhosts.size != 1");
		
		String[] dividedAddr = allhosts[0].split(":"); 
		if(dividedAddr.length != 2) throw new CommException("Please enter an address in the following representation addr:port"); 
		String ipAddr = dividedAddr[0];
		int port = Integer.parseInt(dividedAddr[1]);
		
		_debug = SystemProperties._debug;
		
		try
		{
			this._socket = new Socket(ipAddr, port);
		    Utils.setSocketProperties(_socket);
			System.out.println("Connection established...");

	        System.out.println("Connection accepted " +
	        		_socket.getInetAddress() + ":" +
	                        _socket.getPort());
	        
			this._oos = new ObjectOutputStream(_socket.getOutputStream());
			this._oos.flush();
			this._ois = new ObjectInputStream(_socket.getInputStream());
			
			//this._addr = _socket.getLocalSocketAddress().toString();
			this._addr = InetAddress.getLocalHost().getHostAddress();
			if(_debug) System.out.println("my local address: "+_addr);
			
			this._receiver = new ReceiverThread("Receiver");
			_receiver.start();
		}
		catch(IOException ioe)
		{
			throw new CommException(ioe);
		}
	}
	
	@Override
	public void cleanup()
	{
		try {
			if(_debug) 	
			{
				System.out.println("Closing SimpleTCPComm...");
				System.out.println(ClientComm.mapElems());
			}
			 
			_oos.flush();
			_oos.close();
			_ois.close();
			_socket.close();
			_receiver.close();
			
			if(_debug) System.out.println("SimpleTCPCom closed...");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public String getAddr() {
		return _addr;
	}
	
	@Override
	public String getReaderAddr() {
		return getAddr();
	}
	
	@Override
	public void sendOneWay(IClientOperation op, String addr) throws CommException {
		sendOneWay(op);
	}
	
	@Override
	public void sendOneWay(IClientOperation op) throws CommException
	{
		try{
			//System.out.println(op.toString()+"\n");
			//long st = System.currentTimeMillis();
			op.setVersionVector(_service.getTimeVector());
//			op.opValidation();
//			if (_debug ) System.out.println(op.toString());
			op.serialize(_oos);
			//long en = System.currentTimeMillis();
			//System.out.println("Serializing: "+(en-st));
		} 
		catch (IOException e) {
			throw new CommException(e);
		} /*catch (OpException e) {
			throw new CommException(e);
		}*/
	}
	
	@Override
	public int sendMessage(IClientOperation op, IAsyncCallback cb) throws CommException
	{
		int seqNumber = op.getOpSeq();
//		if(_debug) System.out.println(id);
		ClientComm.addCallback(cb, seqNumber);
//		op.setID(id);
//		op.setOpSeq(seqNumber);
		sendOneWay(op);
		return seqNumber;
	}

	@Override
	public int sendReadMessage(IClientOperation op, IAsyncCallback cb)
			throws CommException {
		return sendMessage(op, cb);
	}
	
	private class ReceiverThread extends Thread
	{
		public boolean close = false;
		
		public ReceiverThread(String name)
		{
			super(name);
		}
		
		public IResult getResult() throws TCPException
		{
			try {
				//System.out.println("SocketOpen?: "+isOpen());
				int type = _ois.readInt();
				IResult result = null;
				
				switch(type)
				{
					case 5: 
						result = new UpdateResult(_ois);
						break;
					case 6:
						result = new ReadResult(_ois);
						break;
					case 7:
						result = new ScanResult(_ois);
						break;
					default: 
						throw new TCPException("TSocket - Type of Result not expected");
				}
				return result;
			} catch (IOException e) {
				throw new TCPException(e);
			} catch (ClassNotFoundException e) {
				throw new TCPException(e);
			}
		}
		
		public void run()
		{
			IResult r = null;
			ProxyService p;
			try {
				p = ProxyService.getInstance();

				while(!close)
				{
					try {
						r = getResult();
	//					if(_debug) System.out.println("while: "+r.getID());
						
						try {
							p.updateTimeVector(r.getVersionVector());
						} catch (Exception e) {
							System.err.println(e.getMessage());
						}
						
						IAsyncCallback cb = ClientComm.getRegisteredCallback(r.getOpSeq());
						if(cb != null) cb.response(r);
	
	//					if(_debug) System.out.println("Continue...");
					} catch (TCPException e) {
	//					if(_debug) e.printStackTrace();
	//					throw new RuntimeException(e);
					}
				}
	//			if(_debug) System.out.println("End of road...");
			} catch (CException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		public void close()
		{
			close = true;
			this.interrupt();
		}
	}

	@Override
	public int getNumberServers() {
		// TODO Auto-generated method stub
		return 1;
	}	
}
