package pt.fct.di.serverProxy.comm.external;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import pt.fct.di.ops.Delete;
import pt.fct.di.ops.IClientOperation;
import pt.fct.di.ops.IResult;
import pt.fct.di.ops.Put;
import pt.fct.di.ops.OpException;
//import pt.fct.di.ops.Operation;
import pt.fct.di.ops.Read;
//import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.Scan;

import pt.fct.di.serverProxy.comm.CommException;
import pt.fct.di.serverProxy.service.MessageDispatcher;
import pt.fct.di.serverProxy.service.ServiceException;
import pt.fct.di.util.Constants;
import pt.fct.di.util.Utils;

public class SimpleTCPComm extends IServerComm{
	
	/**
	 * Server socket to accept client connections.
	 */
	private ServerSocket _serverSocket = null;

	/**
	 * Handler to receive incoming communications.
	 */
	private HReceiver _receiver = null;
	
	public void initServer() throws CommException
	{
		//init();
		
		String addr = getProperties().getProperty("midserverIP", Constants.DEFAULT_TCP_IP);
		String port = getProperties().getProperty("midserverPort", Constants.DEFAULT_TCP_PORT);
		System.out.println("Binded to: "+addr+":"+port);
		int serverport = Integer.parseInt(port);
		
		try {
			this._serverSocket = new ServerSocket();
			this._serverSocket.bind(new InetSocketAddress(addr, serverport));
			Utils.setServerSocketProperties(_serverSocket);
			addr = null;
			port = null;
			
			_receiver = new HReceiver();
			_receiver.start();
		} catch (IOException e) {
			CommException ce = new CommException(e.getMessage());
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
	}
	
	public void cleanup()
	{
//		_dispatcher.close();
		try {
			_serverSocket.close();
//			_pool.destroy();
			_receiver.interrupt();		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
//			_dispatcher = null;
			_serverSocket = null;
//			_pool = null;
			_receiver = null;
		}
	}
	
	/**
	 * Handler to receive and treat all data received from a client
	 * @author andre_goncalves@di
	 *
	 */
	class HClientComm extends Thread //implements Runnable
	{
		/**
		 * Communication channel abstraction between a client and this server 
		 */
		Socket _client;
		
		/**
		 * Input channel
		 */
		ObjectInputStream _inputStream;
		
		/**
		 * Output channel
		 */
		ObjectOutputStream _outputStream;
		
		/**
		 * Maintains the state if this socket connection
		 */
		boolean _connected = false;
		
//		int _countTout = 0;
		
		/**
		 * Statistics - Count number of successful writes done in this DB. 
		 */
		int _countWrites = 0;
		
		/**
		 * Statistics - Count number of successful reads done in this DB. 
		 */
		int _countReads = 0;
		
		Map<Integer, int[]> _returnedCodes;
		
//		/**
//		 * Database abstraction layer
//		 */
//		DB _db;
		
		/**
		 * Message Dispatcher to treat message received from this client
		 */
		MessageDispatcher _dispatcher;
		
		public HClientComm(Socket cl) throws IOException
		{
			this._client = cl;
			this._outputStream = new ObjectOutputStream(cl.getOutputStream());
			this._outputStream.flush();
			this._inputStream = new ObjectInputStream(cl.getInputStream());
			this._connected = true;
			
			this._returnedCodes = new HashMap<Integer, int[]>();
			
//			try {
				_dispatcher = getDispatcher();
//				_db = DBFactory.newDB(getProperties());
//				_db.init();
//			} catch (UnknownDBException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (DBException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		
		/**
		 * Get Operation from inputStream.
		 * @param in
		 * @return
		 * @throws IOException
		 * @throws ClassNotFoundException
		 * @throws CommException
		 */
		public IClientOperation readOp(ObjectInputStream in) throws IOException, 
		ClassNotFoundException, CommException
		{
			int type = in.readInt(); //read type of a received operation
//			System.out.println("Operation type: "+type);

			if(type == 1) return new Put(in);
			else if(type == 2) return new Delete(in);
			else if(type == 3) return new Read(in);
			else if(type == 4) return new Scan(in);
			else throw new CommException("Operation not recognized! This will be ignored...");
		}
		
		public void run()
		{
			IClientOperation op = null;
			IResult result = null;
			while(_connected)
			{
				//System.out.println("Connected: "+_connected);
				try{
					op = readOp(_inputStream);
					result = _dispatcher.executeClientOperation(op);
					sendMessage(result);
				} catch (IOException e) {
					//TODO Auto-generated catch block
					//e.printStackTrace();
					System.out.println("IOException");
//					e.printStackTrace();
					closeConnection();
					
				} catch(CommException e) {
					System.out.println("CommException");
//					try {
//						if(op != null) sendMessage(new Result(op.getType(), op.getID(), -2, e.getMessage(), op.getTS()));
//					} catch (Exception e1) {
//						//e1.printStackTrace();
//						closeConnection();
//					}
				} catch (OpException e) {
					System.out.println("OpException");
//					e.printStackTrace();
//					try{
//						if(op != null) sendMessage(new Result(op.getType(), op.getID(), -2, e.getMessage(), op.getTS()));
//					} catch (Exception e1) {
//						//e1.printStackTrace();
//						closeConnection();
//					}
				} catch (ServiceException e) {
					System.out.println("ServiceException");
//					try{
//						if(op != null) sendMessage(new Result(op.getType(), op.getID(), -2, e.getMessage(), op.getTS()));
//					} catch (Exception e1) {
//						//e1.printStackTrace();
//						closeConnection();
//					}
				} catch (ClassNotFoundException e) {
					System.out.println("ClassNotFoundException");
//					try{
//						if(op != null) sendMessage(new Result(op.getType(), op.getID(), -2, e.getMessage(), op.getTS()));
//					} catch (Exception e1) {
//						//e1.printStackTrace();
//						closeConnection();
//					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//				finally {
//					op = null;
//					result = null;
//				}
			}
		}
		
		public void sendMessage(IResult result) throws IOException, OpException
		{
			//System.out.println("Sending result: \n"+result.toString());
//			result.opValidation();
			result.serialize(_outputStream);
		}
		
		public void closeConnection()
		{
			System.out.println("Closing client connection...");
			try {
				//_db.cleanup();
				_outputStream.flush();
				_outputStream.close();
				_inputStream.close();
				_client.close();
				System.out.println("Client connection closed...");
			} catch (IOException e) {
				//e.printStackTrace();
			}
//			} catch (DBException e) {
//				e.printStackTrace();
//			}
			finally
			{
				_connected = false;
				_outputStream = null;
				_inputStream = null;
				_client = null;
				
//+++++++++++++++++++++++++++Statistics+++++++++++++++++++++++++++++++//
				
//				System.out.println("Number of TimeOuts: "+_countTout);
//				System.out.println("***********************************");
//				System.out.println("*            Statistics           *");
//				System.out.println("***********************************\n");
//				System.out.println("Number of successful writes: "+_countWrites+" ops");
//				System.out.println("Number of successful reads: "+_countReads+" ops");
//				for(Map.Entry<Integer, int[]> tuple: _returnedCodes.entrySet())
//				{
//					System.out.println("Code: "+tuple.getKey()+", Times: "+tuple.getValue()[0]);
//				}
			}
		}
	}
	
	/**
	 * Handler to receive incoming communications from 
	 * @author andre_goncalves@di
	 *
	 */
	class HReceiver extends Thread
	{
		public void run()
		{
			try {
				while(true)
				{
					System.out.println("Waiting...");
					Socket client = _serverSocket.accept();
					System.out.println("Connected...");
					Utils.setSocketProperties(client);

					HClientComm hclient = new HClientComm(client); //TODO: See if it is necessary to set new properties. 
					hclient.start();
					//_pool.executeWork(hclient);
				}
			} catch (IOException e) { //TODO: See this...
				//e.printStackTrace();
			}
		}
	}
}
