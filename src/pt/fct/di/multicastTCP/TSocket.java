package pt.fct.di.multicastTCP;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import pt.fct.di.ops.IClientOperation;
import pt.fct.di.ops.IResult;
import pt.fct.di.ops.ScanResult;
import pt.fct.di.ops.UpdateResult;
//import pt.fct.di.ops.Operation;
import pt.fct.di.ops.ReadResult;

/**
 * This object wraps a simple tcp socket
 * @author andre@di
 *
 */
public class TSocket {
	
	private Socket _socket = null;
	private ObjectInputStream _inputStream = null;
	private ObjectOutputStream _outputStream = null;
	
	/**
	 * Remote host
	 */
	private String _host  = null;

	/**
	 * Remote port
	 */
	private int _port = 0;

	/**
	 * Socket timeout
	 */
	private int _timeout = 0;
	
	
	public TSocket(Socket socket) throws TCPException
	{
		this._socket = socket;
		setSocketProperties();
	    
	    if(isOpen())
	    {
	    	try
	    	{
	    		_inputStream = new ObjectInputStream(_socket.getInputStream());
	    		_outputStream = new ObjectOutputStream(_socket.getOutputStream());
	    	}
	    	catch(IOException iox)
	    	{
	    		iox.printStackTrace();
	    		close();
	    	}
	    }
	}
	
	/**
	 * Creates a new unconnected socket that will connect to the given host
	 * on the given port.
	 *
	 * @param host Remote host
	 * @param port Remote port
	 */
	public TSocket(String host, int port) {
		this(host, port, 0);
	}
	
	/**
	 * Creates a new unconnected socket that will connect to the given host
	 * on the given port.
	 *
	 * @param host    Remote host
	 * @param port    Remote port
	 * @param timeout Socket timeout
	 */
	public TSocket(String host, int port, int timeout) {
		_host = host;
		_port = port;
		_timeout = timeout;
		initSocket();
	}
	
	/**
	 * Sets the socket timeout
	 *
	 * @param timeout Milliseconds timeout
	 */
	public void setTimeout(int timeout) {
	  _timeout = timeout;
	  try {
	    _socket.setSoTimeout(timeout);
	  } catch (SocketException sx) {
	    sx.printStackTrace();
	  }
	}

	/**
	 * Returns a reference to the underlying socket.
	 */
	public Socket getSocket() {
	  if (_socket == null) {
	    initSocket();
	  }
	  return _socket;
	}
	
	/**
	 * Initializes the socket object
	 */
	private void initSocket() {
	  _socket = new Socket();
	    try {
	      setSocketProperties();
	    } catch (TCPException sx) {
	    	sx.printStackTrace();
	    }
	  }
	
	/**
	 * Checks whether the socket is connected.
	 */
	public boolean isOpen() 
	{
		if (_socket == null) return false;
		return _socket.isConnected();
	}
	
	/**
	 * Connects the socket, creating a new socket object if necessary.
	 */
	public void open() throws TCPException 
	{
		if (isOpen()) {
			throw new TCPException("Socket to host "+_socket.getInetAddress().getHostAddress()+":"+_socket.getPort() 
					+" already connected.");
		}

		if (_host.length() == 0) {
			throw new TCPException("Cannot open null host.");
		}
		
		if (_port <= 0) {
			throw new TCPException("Cannot open without port.");
		}

		if (_socket == null) {
			initSocket();
		}

		try {
			_socket.connect(new InetSocketAddress(_host, _port), _timeout);
			_outputStream = new ObjectOutputStream(_socket.getOutputStream());
			_outputStream.flush();
			_inputStream = new ObjectInputStream(_socket.getInputStream());

		} catch (IOException iox) {
			close();
			throw new TCPException(iox);
		}
	}
	
	public String getLocalAddress()
	{ 
		return _socket.getInetAddress().getHostAddress();
		//if(this.isOpen()) return _socket.getLocalSocketAddress().toString();
		//else return "does not exist";
	}
	
	public String getRemoteAddress()
	{
		return _host+":"+_port;
	}
	
	public void sendMessage(IClientOperation op) throws TCPException
	{
	  try {
		op.serialize(_outputStream);
	  } catch (IOException e) {
		throw new TCPException(e);
	  }
	}
	
	public IResult receiveMessage() throws TCPException
	{
		try {
			//System.out.println("SocketOpen?: "+isOpen());
			int type = _inputStream.readInt();
			IResult result = null;
			
			switch(type)
			{
				case 5: 
					result = new UpdateResult(_inputStream);
					break;
				case 6:
					result = new ReadResult(_inputStream);
					break;
				case 7:
					result = new ScanResult(_inputStream);
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
	
	public IResult sendReceiveMessage(IClientOperation op) throws TCPException
	{
		sendMessage(op);
		return receiveMessage();
	}
	
	/**
	 * Closes the socket.
	 */
	public void close() {

	  // Close the socket
	  if (_socket != null) {
	    try {
	    	if(_outputStream != null)
	    	{	
	    		_outputStream.flush();
	    		_outputStream.close();
	    	}
	    	if(_inputStream != null) _inputStream.close();
	    	_socket.close();
	    } catch (IOException iox) { }
	    
	    _socket = null;
	  }
	}
	
	private void setSocketProperties() throws TCPException
	{
	    try {
			_socket.setSoLinger(false, 0);
		    _socket.setTcpNoDelay(true);
		    _socket.setSoTimeout(0);
		    _socket.setKeepAlive(false);
		    _socket.setSendBufferSize(10024);
		    _socket.setReceiveBufferSize(10024);
		} catch (SocketException e) {
			throw new TCPException(e);
		}
	}
}
