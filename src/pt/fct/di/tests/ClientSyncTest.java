package pt.fct.di.tests;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.multicastTCP.TCPException;
import pt.fct.di.ops.Put;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.util.Utils;

public class ClientSyncTest {

	/**
	 * @param args
	 */
	public final int CLIENT_ID = 1;
	
	public Socket _socket;
	public ObjectOutputStream _outputStream;
	public ObjectInputStream _inputStream;
	
	public void init(String addrIp, int port) throws UnknownHostException, IOException, TCPException
	{	
//		ServerSocket ss = new ServerSocket();
//		ss.bind(new InetSocketAddress(addrIp, port));
//		Utils.setServerSocketProperties(ss);
		Socket server = new Socket(addrIp, port);
		Utils.setSocketProperties(server);
		
		_outputStream = new ObjectOutputStream(server.getOutputStream());
		_outputStream.flush();
		_inputStream = new ObjectInputStream(server.getInputStream());
	}
	
	public void runTest(int startPoint, int nRecords)
	{
		assert startPoint < nRecords : "StartPoint must be smaller than nRecords";
		
		long[] versionVector = new long[10];
		HashMap<String,String> map = null;
		for(int i=startPoint; i<nRecords; i++)
		{
			map = new HashMap<String,String>();
			map.put("a"+i, "v"+i);
			Put put = new Put(CLIENT_ID, i, "data", "k7", map, ConsistencyLevel.ONE);
			put.setVersionVector(versionVector);
			try {
				System.out.println(put.toString());
				put.serialize(_outputStream);
				_outputStream.flush();
				UpdateResult res = new UpdateResult(_inputStream);
				System.out.println(res.toString());
				int code = res.getCode();
				if(code >= 0) versionVector[CLIENT_ID] = versionVector[CLIENT_ID] + 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		ClientSyncTest test = new ClientSyncTest();
		try {
			test.init(args[0], Integer.parseInt(args[1]));
			test.runTest(Integer.parseInt(args[2]), Integer.parseInt(args[3]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TCPException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
