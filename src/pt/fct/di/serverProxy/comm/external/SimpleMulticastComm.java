package pt.fct.di.serverProxy.comm.external;

//import java.io.ByteArrayInputStream;
import java.io.IOException;
//import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
//import java.util.HashMap;
//import java.util.Vector;

//import pt.fct.di.ops.Delete;
//import pt.fct.di.ops.Put;
import pt.fct.di.ops.OpException;
//import pt.fct.di.ops.Read;
import pt.fct.di.ops.ReadResult;
//import pt.fct.di.ops.Scan;
import pt.fct.di.serverProxy.SException;
import pt.fct.di.serverProxy.comm.CommException;
//import pt.fct.di.serverProxy.service.ServerService;
import pt.fct.di.util.Constants;

//TODO: CHANGE THIS CLASS TO THE NEWER VERSION OF THE SERVER CODE

public class SimpleMulticastComm extends IServerComm{
	
	private MulticastSocket _multicastSocket;
	private int _multicastPort;
	private DatagramSocket _unicastSocket;
	private int _unicastPort;
	
	private Receiver _receiveThread;
	
	private byte[] _receiveBuffer;
	//private byte[] _sendBuffer;

	public void initServer() throws CommException
	{
		String mIp = getProperties().getProperty("multicastIP", Constants.DEFAULT_MULTICAST_IP);
		String mPort = getProperties().getProperty("multicastPort", Constants.DEFAULT_MULTICAST_PORT);
		this._multicastPort = Integer.parseInt(mPort);
		
		String uPort = getProperties().getProperty("unicastPort", Constants.DEFAULT_UNICAST_PORT);
		this._unicastPort = Integer.parseInt(uPort);
	
		try
		{
			InetAddress group = InetAddress.getByName(mIp);
			this._multicastSocket = new MulticastSocket(_multicastPort);
			this._multicastSocket.joinGroup(group);
			
			this._unicastSocket = new DatagramSocket();
			this._receiveThread = new Receiver();
			
			this._receiveBuffer = new byte[65536];
			
			_receiveThread.run();
		}
		catch(IOException ioe)
		{
			CommException ce = new CommException(ioe.getMessage());
			ce.setStackTrace(ioe.getStackTrace());
			throw ce;
		}
	}
	
	public void cleanup()
	{
		_receiveThread.interrupt();
		_multicastSocket.close();
		_unicastSocket.close();
	}
	
	public void treatMsg(byte[] msg, InetAddress dest)
	{
		/*try{
			ByteArrayInputStream bais = new ByteArrayInputStream(msg);
			ObjectInputStream ois = new ObjectInputStream(bais);
			
			String type = ois.readUTF(); //read type of a received operation
			//System.out.println("Msg Recevied. TYPE: "+type);
	
			ServerService service = null;//getService();
			Result result = null;
			
			if(type.equals("read")){
				result = service.read(new Read(ois));
				//Read r = new Read(ois);
				//System.out.println("Read " + (finalTime-r.getTS()) + " " + msg.getBuffer().length);
				//result = new Result(r.getType(), r.getID(), 1, new HashMap<String, String>(), System.currentTimeMillis());
			}
			else if(type.equals("scan")){
				result = service.scan(new Scan(ois));
				//Scan s = new Scan(ois);
				//System.out.println("Scan " + (finalTime-s.getTS()) + " " + msg.getBuffer().length);
				//result = new Result(s.getType(), s.getID(), 1, new Vector<HashMap<String, String>>(), System.currentTimeMillis());
			}
			else if(type.equals("insert")){
				result = service.insert(new Insert(ois));
				//Insert i = new Insert(ois);
				//System.out.println("Insert " + (finalTime-i.getTS()) + " " + msg.getBuffer().length);
				//result = new Result(i.getType(), i.getID(), 1, "", System.currentTimeMillis());
			}
			else if(type.equals("update")){
				result = service.update(new Update(ois));
				//Update u = new Update(ois);
				//System.out.println("Update " + (finalTime-u.getTS()) + " " + msg.getBuffer().length);
				//result = new Result(u.getType(), u.getID(), 1, "", System.currentTimeMillis());
			}
			else if(type.equals("delete")){
				result = service.delete(new Delete(ois));
				//Delete d = new Delete(ois);
				//System.out.println("Delete " + (finalTime-d.getTS()) + " " + msg.getBuffer().length);
				//result = new Result(d.getType(), d.getID(), 1, "", System.currentTimeMillis());
			}
			
			ois.close();
			//service = null;
			
			try
			{
				sendMessage(result, dest);
			} catch (SException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		catch(IOException ioe)
		{
			ioe.printStackTrace();
		} 
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}*/
	}
	
	public void sendMessage(ReadResult result, InetAddress addr) throws IOException, SException
	{
		try
		{	
			//System.out.println(result.toString());
			byte[] sendBuffer = result.toByteArray();
			DatagramPacket data = new DatagramPacket(sendBuffer, 0, sendBuffer.length, addr, _unicastPort);
			_unicastSocket.send(data);
		}
		catch(OpException e)
		{
			SException se = new SException(e.getMessage());
			se.setStackTrace(e.getStackTrace());
			throw se;
		}	
	}
	
	
	class Receiver extends Thread
	{
		public void run()
		{
			while(true)
			{
				try
				{
					DatagramPacket data = new DatagramPacket(_receiveBuffer, 0, _receiveBuffer.length);
					_multicastSocket.receive(data);
					treatMsg(_receiveBuffer, data.getAddress());	
				}
				catch(IOException ioe)
				{
					ioe.printStackTrace();
				}
			}
		}
	}
}
