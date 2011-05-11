package pt.fct.di.util;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class Utils {

	public static void setSocketProperties(Socket socket) throws SocketException
	{
	    socket.setSoLinger(false, 0);
	    socket.setTcpNoDelay(true);
	    socket.setSoTimeout(0);
	    socket.setKeepAlive(false);
	    socket.setSendBufferSize(10024);
	    socket.setReceiveBufferSize(10024);
	}
	
	public static void setServerSocketProperties(ServerSocket ssocket) throws SocketException{
	    ssocket.setReuseAddress(true);
        ssocket.setSoTimeout(0);
	}
}
