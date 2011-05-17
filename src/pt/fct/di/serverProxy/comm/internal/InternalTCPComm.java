package pt.fct.di.serverProxy.comm.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pt.fct.di.ops.log.DeleteLog;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.ops.log.PutLog;

import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.util.Utils;

public class InternalTCPComm extends IComm{
	
	/**
	 * Server socket to accept client connections.
	 */
	private ServerSocket _serverSocket = null;

	/**
	 * Handler to receive incoming communications.
	 */
//	private HReceiver _receiver = null;
	
	/**
	 * Collection to store severs addresses 
	 */
	private String[] _serversAddr = null;
	
	private static InternalTCPComm _instance = null;
	
	public void initServer() throws CommException
	{
		//init();
		if(!getProperties().containsKey("serversInternalAddresses")) 
			throw new CommException("Field \"serversInternalAddresses\" is missing. Please set this property in config file");
		
		_serversAddr = getProperties().getProperty("serversInternalAddresses").split(",");
		String[] ownAddr = _serversAddr[0].split(":"); 
		String ipAddr = ownAddr[0]; 
		int port = Integer.parseInt(ownAddr[1]); 
		
		System.out.println("Internal Comm Binded to: "+ipAddr+":"+port);
		
		try {
			this._serverSocket = new ServerSocket();
			this._serverSocket.bind(new InetSocketAddress(ipAddr, port));
			Utils.setServerSocketProperties(_serverSocket);
			ownAddr = null;
			ipAddr = null;
			
//			_receiver = new HReceiver();
//			_receiver.start();
		} catch (IOException e) {
			CommException ce = new CommException(e.getMessage());
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
		finally
		{
			_instance = this;
		}
	}
	
	public void cleanup()
	{
		//_dispatcher.close();
		try {
			_serverSocket.close();
//			_receiver.interrupt();		
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			//_dispatcher = null;
			_serverSocket = null;
//			_receiver = null;
		}
	}
	
	public static InternalTCPComm getInstance() throws CommException
	{
		if(_instance == null) throw new CommException("Instance of communication not set");
		return _instance;
	}
	
	public int getNumberServers()
	{
		return _serversAddr.length;
	}
	
	private long[] receiveVector(ObjectInputStream ois) throws IOException
	{
		//read info
		int vectorSize = ois.readInt(); //also indicates the number of clients known between servers
		long [] otherVector = new long[vectorSize];
//		System.out.print("Other version vector: [ ");
		for(int pos = 0; pos < vectorSize; pos++) 
		{
			otherVector[pos] = ois.readLong();
//			System.out.print(otherVector[pos]+", ");
		}
//		System.out.println(" ]");
		return otherVector;
	}
	
	private void writeOperationsRequest(ObjectOutputStream oos, Map<Integer,Long> requestMap) throws IOException
	{
		oos.writeInt(requestMap.size());
		for(Map.Entry<Integer,Long> entry : requestMap.entrySet())
		{
			oos.writeInt(entry.getKey());
			oos.writeLong(entry.getValue());
		}
	}
	
	private Map<Integer,List<ILogOperation>> readSyncMap(ObjectInputStream ois) throws IOException
	{
		int clientId = -1;
		int numberOps = -1;
		int opType = -1;
		Map<Integer,List<ILogOperation>> syncMap = null;
		List<ILogOperation> opsToSync = null;
		
		int size = ois.readInt();
//		System.out.println("Size of Map: "+size);
		syncMap = new HashMap<Integer,List<ILogOperation>>(size);
		for(int nElem=0; nElem < size; nElem++)
		{
			clientId = ois.readInt();
//			System.out.println("clientId: "+clientId);
			numberOps = ois.readInt();
//			System.out.println("NumberOps: "+numberOps);
			opsToSync = new ArrayList<ILogOperation>(numberOps);
			for(int opPos = 0; opPos < numberOps; opPos++)
			{
				opType = ois.readInt();
//				System.out.println("OpType: "+opType);
				if(opType == 1) opsToSync.add(new PutLog(ois));
				else if(opType == 2) opsToSync.add(new DeleteLog(ois));
			}
			syncMap.put(clientId, opsToSync);
		}
		return syncMap;
	}
	
	private void synchronize(ObjectInputStream ois, ObjectOutputStream oos) throws IOException
	{
		try
		{
			System.out.println("\n= Starting Sync Protocol initiated by other=\n");
			int otherServerId = ois.readInt();
			long [] otherVector = receiveVector(ois);
			
			//see if it is synchronized
			Map<Integer,Long> requestMap = new HashMap<Integer,Long>(otherVector.length);
			boolean updated = _dispatcher.verifyVersionVector(otherServerId, otherVector, requestMap);
			
//			System.out.println("=======================");
//			System.out.println("|| Operations to Sync||");
//			System.out.println("=======================");
//			System.out.println("SyncMap size: "+requestMap.size());
//			for(Map.Entry<Integer,Long> entry : requestMap.entrySet())
//				System.out.println("ClientId: "+entry.getKey()+", Timestamp: "+entry.getValue());
			
			//write request
//			System.out.println("\n= Writing Request to Sync =\n");
			if(updated) oos.writeBoolean(true);
			else
			{
				oos.writeBoolean(false);
				writeOperationsRequest(oos, requestMap);
				oos.flush();
				
				//read response to early request in 8
//				System.out.println("\n= Applying Sync Operations =\n");
				Map<Integer,List<ILogOperation>> syncMap = this.readSyncMap(ois);
				_dispatcher.applyOperations(syncMap);
			}
			System.out.println("\n= Finishing Sync Protocol initiated by other=\n");
		} catch (InterruptedException e) {
			System.out.println("HInternalComm - synchronize() thrown InterruptedException");
		}
	}
	
//	public void treatInternalMessage(ObjectInputStream ois, ObjectOutputStream oos) throws IOException
//	{
//		int messageType = ois.readInt();
//		
//		switch(messageType)
//		{
//		case 8: 
//			synchronize(ois,oos); //done
//			break;
//		default:
//			break;
//		}
//	}

//	public void synchronize(int myId, long[] versionVector)
//	{
//		if(_serversAddr.length == 1) return;
//		try
//		{
//			HInternalComm[] servers = new HInternalComm[_serversAddr.length];
//			for(int nServer = 1; nServer < _serversAddr.length; nServer++)
//			{
//				servers[nServer] = new HInternalComm(_serversAddr[nServer],myId,versionVector);
//				servers[nServer].start();
//			}
//			
//			for(int nServer = 1; nServer < servers.length; nServer++)
//			{
//				servers[nServer].join();
//			}
//		}catch(IOException ioe)
//		{
//			System.out.println("InternalTCPComm - synchronize() thrown IOException.");
//		} catch (InterruptedException e) {
//			System.out.println("InternalTCPComm - synchronize() thrown InterruptedException.");
//		}
//	}
//	
//	class HInternalComm extends Thread
//	{
//		private final int SYNCHRONIZE_MESSAGE_ID = 8;
//		
//		public Socket _server;
//		public ObjectInputStream _ois;
//		public ObjectOutputStream _oos;
//		public int _myId;
//		public long[] _versionVector;
//		
//		HInternalComm(String addr, int myid, long[] vector) throws NumberFormatException, UnknownHostException, IOException
//		{
//			String[] addrInfo = addr.split(":");
//			_server = new Socket(addrInfo[0], Integer.parseInt(addrInfo[1]));
//			Utils.setSocketProperties(_server);
//			_oos = new ObjectOutputStream(_server.getOutputStream());
//			_oos.flush();
//			_ois = new ObjectInputStream(_server.getInputStream());
//			_myId = myid;
//			_versionVector = vector;
//		}
//		
//		private void startSyncProtocol() throws IOException
//		{
//			System.out.println("\n= Starting Sync Protocol =\n");
//			
//			_oos.writeInt(SYNCHRONIZE_MESSAGE_ID);
//			_oos.writeInt(_myId);
//			_oos.writeInt(_versionVector.length);
//			for(int pos = 0; pos < _versionVector.length; pos++)
//				_oos.writeLong(_versionVector[pos]);
//			_oos.flush();
//		}
//		
//		private Map<Integer,Long> receiveRequest() throws IOException
//		{
////			//read info
////			System.out.println("\n= Receive Request from other server =\n");
//			
//			int size = _ois.readInt();
//			Map<Integer,Long> requestList = new HashMap<Integer,Long>(size);
//			for(int pos = 0; pos < size; pos++) 
//				requestList.put(_ois.readInt(),_ois.readLong());
//			
////			System.out.println("=======================");
////			System.out.println("|| 	 Request List 	 ||");
////			System.out.println("=======================");
////			System.out.println("SyncMap size: "+requestList.size());
////			for(Map.Entry<Integer,Long> entry : requestList.entrySet())
////				System.out.println("ClientId: "+entry.getKey()+", Timestamp: "+entry.getValue());
//			
//			return requestList;
//		}
//		
//		private void writeOperationsToSync(Map<Integer,List<ILogOperation>> syncMap) throws IOException
//		{			
//			//write results
////			System.out.println("\n= Write Operations to Synchronize =\n");
//			
//			_oos.writeInt(syncMap.size());
//			for(Map.Entry<Integer, List<ILogOperation>> entry : syncMap.entrySet())
//			{
//				_oos.writeInt(entry.getKey());
//				_oos.writeInt(entry.getValue().size());
//				for(ILogOperation op : entry.getValue()) op.serialize(_oos);
//			}
//			_oos.flush();
//		}
//		
//		public void run()
//		{
//			try {
//				startSyncProtocol();
//				boolean otherServerUpdated = _ois.readBoolean();
////				System.out.println("Updated? : "+otherServerUpdated); 
//				if(!otherServerUpdated)
//				{
//					Map<Integer,List<ILogOperation>> opsToSync = _dispatcher.getSyncOperations(receiveRequest());
//					writeOperationsToSync(opsToSync);
//				}
//				System.out.println("\n= Finishing Sync Protocol =\n");
//			} catch (IOException e) {
//				System.out.println("HInternalComm - run() thrown IOException");
//			} catch (InterruptedException e) {
//				System.out.println("HInternalComm - run() thrown InterruptedException");
//			}
//			finally
//			{
//				closeConnection();
//			}
//		}
//		
//		public void closeConnection()
//		{
//			try {
//				_oos.flush();
//				_oos.close();
//				_ois.close();
//				_server.close();
//			} catch (IOException e) {
//				System.out.println("HInternalComm - closeConnection() thrown IOException");
//			}
//
//		}
//	}
	
	
//	/**
//	 * Handler to receive and treat all data received from a client
//	 * @author andre_goncalves@di
//	 *
//	 */
//	class HInternalComm extends Thread //implements Runnable
//	{
//		/**
//		 * Communication channel abstraction between a client and this server 
//		 */
//		Socket _client;
//		
//		/**
//		 * Input channel
//		 */
//		ObjectInputStream _inputStream;
//		
//		/**
//		 * Output channel
//		 */
//		ObjectOutputStream _outputStream;
//		
//		/**
//		 * Maintains the state if this socket connection
//		 */
//		boolean _connected = false;
//		
//		/**
//		 * Statistics - Count number of successful writes done in this DB. 
//		 */
//		int _countWrites = 0;
//		
//		/**
//		 * Statistics - Count number of successful reads done in this DB. 
//		 */
//		int _countReads = 0;
//		
//		Map<Integer, int[]> _returnedCodes;
//		
//		/**
//		 * Message Dispatcher to treat message received from this client
//		 */
//		MessageDispatcher _dispatcher;
//		
//		public HInternalComm(Socket cl) throws IOException
//		{
//			this._client = cl;
//			this._outputStream = new ObjectOutputStream(cl.getOutputStream());
//			this._outputStream.flush();
//			this._inputStream = new ObjectInputStream(cl.getInputStream());
//			this._connected = true;
//			
//			this._returnedCodes = new HashMap<Integer, int[]>();
//			
//			_dispatcher = getDispatcher();
//		}
//		
//		/**
//		 * Get Operation from inputStream.
//		 * @param in
//		 * @return
//		 * @throws IOException
//		 * @throws ClassNotFoundException
//		 * @throws CommException
//		 */
///*		public IClientOperation readOp(ObjectInputStream in) throws IOException, 
//		ClassNotFoundException, CommException
//		{
//			int type = in.readInt(); //read type of a received operation
////			System.out.println("Operation type: "+type);
//
//			if(type == 1) return new Put(in);
//			else if(type == 2) return new Delete(in);
//			else if(type == 3) return new Read(in);
//			else if(type == 4) return new Scan(in);
//			else throw new CommException("Operation not recognized! This will be ignored...");
//		}
//*/		
//		public void run()
//		{
//			//System.out.println("Connected: "+_connected);
//			try{
//				int type = _inputStream.readInt();
//				
//				switch(type)
//				{
//					case 8: //Verify update and return response
//						
//						//read info
//						int otherId = _inputStream.readInt();
//						int vectorSize = _inputStream.readInt(); //also indicates the number of clients known between servers
//						long[] otherVersionVector = new long[vectorSize];
//						System.out.print("Other version vector: [ ");
//						for(int pos = 0; pos < vectorSize; pos++) 
//						{
//							otherVersionVector[pos] = _inputStream.readLong();
//							System.out.print(otherVersionVector[pos]+", ");
//						}
//						System.out.println(" ]");
//						
//						//see if it is synchronized
//						Map<Integer,Long> syncList = new HashMap<Integer,Long>(vectorSize);
//						boolean updated = _dispatcher.verifyVersionVector(otherId, otherVersionVector, syncList);
//						
//						System.out.println("=======================");
//						System.out.println("|| Operations to Sync||");
//						System.out.println("=======================");
//						System.out.println("SyncMap size: "+syncList.size());
//						for(Map.Entry<Integer,Long> entry : syncList.entrySet())
//							System.out.println("ClientId: "+entry.getKey()+", Timestamp: "+entry.getValue());
//						
//						//write request
//						_outputStream.writeInt(9);
//						if(updated) _outputStream.writeBoolean(true);
//						else
//						{
//							_outputStream.writeBoolean(false);
//							_outputStream.writeInt(syncList.size());
//							for(Map.Entry<Integer,Long> entry : syncList.entrySet())
//							{
//								_outputStream.writeInt(entry.getKey());
//								_outputStream.writeLong(entry.getValue());
//							}
//						}
//						_outputStream.flush();
//						break;	
//					case 9: //request operations to sync
//						
//						//read info
//						boolean updateResult = _inputStream.readBoolean();
//						System.out.println("Updated? : "+updateResult);
//						if(!updateResult)
//						{
//							int size = _inputStream.readInt();
//							Map<Integer,Long> requestList = new HashMap<Integer,Long>(size);
//							for(int pos = 0; pos < size; pos++) 
//								requestList.put(_inputStream.readInt(),_inputStream.readLong());
//							
//							System.out.println("=======================");
//							System.out.println("|| 	 Request List 	||");
//							System.out.println("=======================");
//							System.out.println("SyncMap size: "+requestList.size());
//							for(Map.Entry<Integer,Long> entry : requestList.entrySet())
//								System.out.println("ClientId: "+entry.getKey()+", Timestamp: "+entry.getValue());
//							
//							
//							//Get Sync Map with operations to synchronize on the other server
//							Map<Integer,List<ILogOperation>> opsToSync = _dispatcher.getSyncOperations(requestList);
//							
//							System.out.println("=======================");
//							System.out.println("||  Synchronize List ||");
//							System.out.println("=======================");
//							System.out.println("SyncMap size: "+opsToSync.size());
//							for(Map.Entry<Integer,List<ILogOperation>> entry : opsToSync.entrySet())
//							{
//								System.out.print("( ClientId: "+entry.getKey()+", ");
//								for(ILogOperation op : entry.getValue()) System.out.println( op.toString() );
//								System.out.println(" )");
//							}
//							
//							//write results
//							_outputStream.writeInt(10);
//							_outputStream.writeInt(opsToSync.size());
//							for(Map.Entry<Integer, List<ILogOperation>> entry : opsToSync.entrySet())
//							{
//								_outputStream.writeInt(entry.getKey());
//								_outputStream.writeInt(entry.getValue().size());
//								for(ILogOperation op : entry.getValue())
//									op.serialize(_outputStream);
//							}
//							_outputStream.flush();
//						}
//						break;
//					case 10:
//						//TODO: Receive operations and update log, db and version vector
//						int clientId = -1;
//						int numberOps = -1;
//						int opType = -1;
//						
//						//read response to early request in 8
//						int size = _inputStream.readInt();
//						System.out.println("Size of Map: "+size);
//						Map<Integer,List<ILogOperation>> syncMap = new HashMap<Integer,List<ILogOperation>>(size);
//						List<ILogOperation> opsToSync = null;
//						for(int pos=0; pos < size; pos++)
//						{
//							clientId = _inputStream.readInt();
//							System.out.println("clientId: "+clientId);
//							numberOps = _inputStream.readInt();
//							System.out.println("NumberOps: "+numberOps);
//							opsToSync = new ArrayList<ILogOperation>(numberOps);
//							syncMap.put(clientId, opsToSync);
//							for(int opPos = 0; opPos < numberOps; opPos++)
//							{
//								opType = _inputStream.readInt();
//								System.out.println("OpType: "+opType);
//								if(opType == 1) opsToSync.add(new PutLog(_inputStream));
//								else if(opType == 2) opsToSync.add(new DeleteLog(_inputStream));
//							}
//							
//							//Synchronization of operations missing
//							_dispatcher.applyOperations(syncMap); 
//						}
//						break;
//					default: break;
//				}
//				
//			} catch (IOException e) {
//				//TODO Auto-generated catch block
//				//e.printStackTrace();
//				System.out.println("IOException");
////					e.printStackTrace();
//				closeConnection();		
//			}
//		}
//		
//		public void sendMessage(IResult result) throws IOException, OpException
//		{
//			//System.out.println("Sending result: \n"+result.toString());
////			result.opValidation();
//			result.serialize(_outputStream);
//		}
//		
//		public void closeConnection()
//		{
//			System.out.println("Closing internal connection...");
//			try {
//				//_db.cleanup();
//				_outputStream.flush();
//				_outputStream.close();
//				_inputStream.close();
//				_client.close();
//				System.out.println("Client connection closed...");
//			} catch (IOException e) {
//				//e.printStackTrace();
//			}
////			} catch (DBException e) {
////				e.printStackTrace();
////			}
//			finally
//			{
//				_connected = false;
//				_outputStream = null;
//				_inputStream = null;
//				_client = null;
//			}
//		}
//	}
	
	/**
	 * Handler to receive incoming communications from 
	 * @author andre_goncalves@di
	 *
	 */
//	class HReceiver extends Thread
//	{
//		public void run()
//		{
//			try {
//				Socket server = null;
//				ObjectInputStream ois = null;
//				ObjectOutputStream oos = null;
//				
//				while(true)
//				{
//					System.out.println("Waiting...");
//					server = _serverSocket.accept();
//					System.out.println("Connected...");
//					Utils.setSocketProperties(server);
//
//					ois = new ObjectInputStream(server.getInputStream());
//					oos = new ObjectOutputStream(server.getOutputStream());
//					
//					treatInternalMessage(ois,oos);
//					closeConnection(ois,oos,server);
////					HInternalComm hclient = new HInternalComm(server); //TODO: See if it is necessary to set new properties. 
////					hclient.start();
//				}
//			} catch (IOException e) { //TODO: See this...
//				//e.printStackTrace();
//			}
//		}
//		
//		public void closeConnection(ObjectInputStream ois, ObjectOutputStream oos, Socket socket)
//		{
//			System.out.println("Closing internal connection...");
//			try {
//				oos.flush();
//				oos.close();
//				ois.close();
//				socket.close();
//				System.out.println("Client connection closed...");
//			} catch (IOException e) {
//				//e.printStackTrace();
//			}
////			} catch (DBException e) {
////				e.printStackTrace();
////			}
//		}
//	}
}
