package pt.fct.di.tests;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pt.fct.di.multicastTCP.TCPException;
import pt.fct.di.ops.log.DeleteLog;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.ops.log.PutLog;
import pt.fct.di.util.Utils;

public class ServerSyncTest {
	
	public final int SERVER_ID = 1;
	
	public Socket _socket;
	public ObjectOutputStream _outputStream;
	public ObjectInputStream _inputStream;
	
	public void init(String addrIp, int port) throws UnknownHostException, IOException, TCPException
	{	
		ServerSocket ss = new ServerSocket();
		ss.bind(new InetSocketAddress(addrIp, port));
		Utils.setServerSocketProperties(ss);
		Socket server = ss.accept();
		Utils.setSocketProperties(server);
		
		_outputStream = new ObjectOutputStream(server.getOutputStream());
		_outputStream.flush();
		_inputStream = new ObjectInputStream(server.getInputStream());
	}
	
//	private void setSocketProperties() throws TCPException
//	{
//	    try {
//			_socket.setSoLinger(false, 0);
//		    _socket.setTcpNoDelay(true);
//		    _socket.setSoTimeout(0);
//		    _socket.setKeepAlive(false);
//		    _socket.setSendBufferSize(10024);
//		    _socket.setReceiveBufferSize(10024);
//		} catch (SocketException e) {
//			throw new TCPException(e);
//		}
//	}
	
	public void verifyAlgorithm(long[] vector) throws IOException
	{
		//Test1 - send message with timestamp (normal exchange of timestamps) (Check)
		
//		_oos.writeInt(8);
//		_oos.writeInt(SERVER_ID);
//		_oos.writeInt(vector.length);
//		for(int pos = 0; pos < vector.length; pos++)
//			_oos.writeLong(vector[pos]);
//		_oos.flush();
//		
//		//response of the other server saying if it is updated or not and in the later case asking for updates
////		int msg = _ois.readInt();
//		boolean updated = _ois.readBoolean();
//		System.out.println("Updated? "+updated);
//		if(!updated)
//		{
//			int size = _ois.readInt();
//			Map<Integer,Long> syncList = new HashMap<Integer,Long>(size);
//			for(int nElems = 0; nElems < size; nElems++)
//				syncList.put(_ois.readInt(), _ois.readLong());
//			
//			System.out.println("========================");
//			System.out.println("|| Operations to Sync ||");
//			System.out.println("========================");
//			for(Map.Entry<Integer,Long> entry : syncList.entrySet())
//				System.out.println("ClientId: "+entry.getKey()+", Timestamp: "+entry.getValue());
//		}
//		
//		//send operations to synchronize
//		
//		//fake operations
//		List<ILogOperation> opList = new ArrayList<ILogOperation>();
//		Map<String,String> values = new HashMap<String,String>();
//		values.put("a5", "v20");
//		values.put("a7", "v30");
//		values.put("a10", "v40");
//		opList.add(new PutLog(1, "data", "k7", values, 5));
//		Map<String,String> values2 = new HashMap<String,String>();
//		values2.put("a5", "v5");
//		values2.put("a7", "v3");
//		values2.put("a10", "v4");
//		opList.add(new PutLog(1, "data", "k8", values2,6));
//		
////		_oos.writeInt(10); //msg id
//		_oos.writeInt(1); //number of clients
//		_oos.writeInt(1); //client id
//		_oos.writeInt(opList.size());
//		for(ILogOperation op : opList) op.serialize(_oos);
		
//--------------------------------------------------------------------------------		
		
		//Test2 - Await to receipt other server's version vector and test if I'm updated. Send request to synchronize (Check)
		
//		//read info
		int msgId = _inputStream.readInt();
		int otherId = _inputStream.readInt();
		int vectorSize = _inputStream.readInt(); //also indicates the number of clients known between servers
		long[] otherVersionVector = new long[vectorSize];
		System.out.print("Other version vector: [ ");
		for(int pos = 0; pos < vectorSize; pos++) 
		{
			otherVersionVector[pos] = _inputStream.readLong();
			System.out.print(otherVersionVector[pos]+", ");
		}
		System.out.println(" ]");
		
		//see if it is synchronized
		Map<Integer,Long> syncList = new HashMap<Integer,Long>(vectorSize);
		boolean updated = false;
//		Map<Integer,Long> _syncList = new HashMap<Integer,Long>(2);
		syncList.put(0, (long)0);
		syncList.put(1, (long)2);
		
		System.out.println("=======================");
		System.out.println("|| Operations to Sync||");
		System.out.println("=======================");
		System.out.println("SyncMap size: "+syncList.size());
		for(Map.Entry<Integer,Long> entry : syncList.entrySet())
			System.out.println("ClientId: "+entry.getKey()+", Timestamp: "+entry.getValue());
		
//		_oos.writeInt(9);
		_outputStream.writeBoolean(updated);
		_outputStream.writeInt(syncList.size());
		for(Map.Entry<Integer, Long> entry : syncList.entrySet())
		{
			_outputStream.writeInt(entry.getKey());
			_outputStream.writeLong(entry.getValue());
		}
		
		_outputStream.flush();
		
//		int code = _ois.readInt();
		int mapSize = _inputStream.readInt();
		int listSize = -1;
		int opType = -1;
		for(int nElem = 0; nElem<mapSize; nElem++)
		{
			System.out.print("( ClientId"+_inputStream.readInt()+", ");
			listSize = _inputStream.readInt();
			for(int pos = 0; pos < listSize; pos++)
			{
				opType = _inputStream.readInt();
				ILogOperation op = null;
				if(opType == 1) op = new PutLog(_inputStream);
				else op = new DeleteLog(_inputStream);
				System.out.println(op.toString());
			}
		}
	}

	public static void main(String[] args)
	{
		ServerSyncTest test = new ServerSyncTest();
		try {
			long[] vector = {4,6,0,0,0,0,0,0,0,0}; 			
			test.init(args[0], Integer.parseInt(args[1]));
			test.verifyAlgorithm(vector);
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
