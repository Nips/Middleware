package pt.fct.di.clientProxy.comm;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

import pt.fct.di.client.CException;
import pt.fct.di.ops.OpException;
import pt.fct.di.ops.Operation;
import pt.fct.di.ops.ReadResult;
import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.util.Constants;

public class SimpleMulticastComm extends ClientComm{
	
	private MulticastSocket _multicastSocket;
	private int _multicastPort;
	private InetAddress _multicastGroup;
	private DatagramSocket _unicastSocket;
	private byte[] _msgBuffer;

	public void init() throws CException
	{
		String ip = getProperties().getProperty("multicastIP", Constants.DEFAULT_MULTICAST_IP);
		String port = getProperties().getProperty("multicastPort", Constants.DEFAULT_MULTICAST_PORT);
		this._multicastPort = Integer.parseInt(port);
	
		try
		{
			
			this._multicastGroup = InetAddress.getByName(ip);
			this._multicastSocket = new MulticastSocket(_multicastPort);
			this._multicastSocket.joinGroup(_multicastGroup);
			this._unicastSocket = new DatagramSocket(Integer.parseInt(Constants.DEFAULT_UNICAST_PORT));
			
			this._msgBuffer = new byte[65536];
			
		}catch(IOException ioe)
		{
			CException ce = new CException(ioe.getMessage());
			ce.setStackTrace(ioe.getStackTrace());
			throw ce;
		}
	}
	
	public void cleanup()
	{
		_multicastSocket.close();
		_unicastSocket.close();
	}
	
	public void sendOneWay(Operation op) throws CommException
	{
		try {
			System.out.println(op.toString());
			byte[] msg = op.toByteArray();
			DatagramPacket data = new DatagramPacket(msg, msg.length, 
					_multicastGroup, _multicastPort);
			_multicastSocket.send(data);
			
		} catch (OpException e) {
			throw new CommException(e);
		} catch (IOException e) {
			throw new CommException(e);
		}
	}
	
	public ReadResult sendMessageReceive(Operation op) throws CException
	{
		sendMessage(op);
	
		try{
			
			//Receive Result
			DatagramPacket data = new DatagramPacket(_msgBuffer, 0, _msgBuffer.length);
			_unicastSocket.receive(data);
			ReadResult res = new ReadResult(data.getData());
			//System.out.println(res.toString());
			return res;
			
		}catch(IOException e)
		{
			CException ce = new CException(e.getMessage());
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		} catch (ClassNotFoundException e) {
			CException ce = new CException(e.getMessage());
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
	}
}
