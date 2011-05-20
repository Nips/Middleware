package pt.fct.di.multicastTCP;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import pt.fct.di.ops.IClientOperation;
//import pt.fct.di.ops.Operation;
import pt.fct.di.util.SystemProperties;

public class MSocket {

	/**
	 * Array of Transport Objects which encapsulates a TCP socket, one for each server proxy.
	 */
	private Map<String, TransportThread> _proxies = null;
	
	/**
	 * Array of addresses of the remote host to connect.
	 */
	private String[] _remoteHosts = null;
	
	/**
	 * Local address
	 */
	private String _localAddr = null;
	
//	
//	/**
//	 * Remote port 
//	 */
//	private int _port = 0;
//	
//	/**
//	 * Sockets timeout
//	 */
//	private int _timeout = 0;
	
	public MSocket(String[] addrs) throws Exception
	{
	 	_remoteHosts = addrs;
	 	_proxies = new HashMap<String, TransportThread>();
	 	String addr = "";
	 	String ipAddr = "";
	 	int port = 0;
		for(int i = 0; i<addrs.length; i++)
		{
			addr = addrs[i];
			System.out.println(addr);
			String[] dividedAddr = addr.split(":"); 
			if(dividedAddr.length != 2) throw new Exception("Please enter an address in the following representation addr:port"); 
			ipAddr = dividedAddr[0];
			port = Integer.parseInt(dividedAddr[1]);
			if(SystemProperties.getPropertie("clientId").equals("0"))  _proxies.put(addr, new TransportThread(new TSocket(ipAddr, port)));
			else
			{
//				_proxies.put(addr, new TransportThread(new TSocket(ipAddr, port),true));
				if(i == 0) _proxies.put(addr, new TransportThread(new TSocket(ipAddr, port)));
				else _proxies.put(addr, new TransportThread(new TSocket(ipAddr, port),true));
			}
		}		
	}
	
	public boolean isOpen(){
		boolean state = true;
		for(int i=0; i<_remoteHosts.length; i++) state = state && _proxies.get(_remoteHosts[i]).getSocket().isOpen();
		return state;
	}
	
	public void open() throws Exception
	{
		for(int i=0; i<_remoteHosts.length; i++) _proxies.get(_remoteHosts[i]).init();
	}
	
	public String getLocalAddr() throws UnknownHostException
	{
		if(_localAddr == null) _localAddr = _proxies.get(_remoteHosts[0]).getSocket().getLocalAddress();
		return _localAddr;
		//return InetAddress.getLocalHost().getHostAddress();
	}
	
	public String getAddrs()
	{
		StringBuilder builder = new StringBuilder(_remoteHosts[0]);
		for(int i=1; i<_remoteHosts.length; i++) builder.append(", "+_remoteHosts[i]);
		return builder.toString();
	}
	
	public void sendMessage(IClientOperation op) throws Exception
	{
		for(int i=0; i<_remoteHosts.length; i++) _proxies.get(_remoteHosts[i]).sendMessage(op);
	}
	
	public void sendMessage(IClientOperation op, String serverAddr) throws Exception
	{
		_proxies.get(serverAddr).sendMessage(op);
	}
	
	public void close()
	{
		for(int i=0; i<_remoteHosts.length; i++) _proxies.get(_remoteHosts[i]).close();
	}
}
