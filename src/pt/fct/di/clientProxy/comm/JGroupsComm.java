package pt.fct.di.clientProxy.comm;

//import java.io.ByteArrayInputStream;
import java.io.IOException;
//import java.io.ObjectInputStream;

import org.jgroups.Address;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import pt.fct.di.client.CException;
import pt.fct.di.ops.OpException;
import pt.fct.di.ops.Operation;
import pt.fct.di.ops.ReadResult;

/**
 * JGroups communication stack implementation in the client side
 * 
 * @author andre_goncalves@di
 *
 */
public class JGroupsComm extends ClientComm{
	
	private JChannel _channel;
	private static Address _addr;
	
	class Receiver extends ReceiverAdapter
	{	
		@Override
		public void receive(Message msg)
		{
			//long finalTime = System.nanoTime();
			if(!msg.getSrc().equals(_addr))
			{
				try{
					long st = System.currentTimeMillis();
					ReadResult result = new ReadResult(msg.getBuffer());
					long en = System.currentTimeMillis();
					System.out.println("Deserializing: "+(en-st));
					//System.out.println(result.getType() + " " + (finalTime-result.getTS()) + " " + msg.getBuffer().length);
					if( isOpRegistered(result.getID()) )
						addResultToQueue(result);
				}
				catch(IOException e){
					//do nothing
				}
				catch(ClassNotFoundException cnfe)
				{
					//do nothing
				}
				catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
				return; //Receiver = Sender, do nothing
	    }
	}
	
	public JGroupsComm()
	{
		super();
	}
	
	public void init() throws CException
	{
		try {
			_channel = new JChannel(getProperties().getProperty("commConfFile", getDefaultConfFile()));
			_channel.setReceiver(new Receiver());
			_channel.connect("test");
			_addr = _channel.getAddress();
		} catch (ChannelException e) {
			CException ce = new CException(e.getMessage());
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
	}
	
	public void cleanup()
	{
		//_responseQueue.notifyAll();
		_syncOps = null;
		_responseQueue = null;
		_channel.clearChannelListeners();
		_channel.close();
	}

	public void sendMessage(Operation op) throws CException
	{
		//System.out.println(op.toString());
		try{
			//op.setTS(System.nanoTime());
			long st = System.currentTimeMillis();
			byte[] buffer = op.toByteArray();
			long en = System.currentTimeMillis();
			System.out.println("Serializing: "+(en-st));
			Message msg = new Message(null, null, buffer);
			_channel.send(msg);
		}
		catch(OpException oe)
		{
			CException ce = new CException("sendMessage - Error while serializing operation "+op.getType()
											+" with ID "+op.getID());
			ce.setStackTrace(oe.getStackTrace());
			throw ce;
		}	
		catch(ChannelNotConnectedException cnce)
		{
			CException ce = new CException("sendMessage - Communication channel is not open");
			ce.setStackTrace(cnce.getStackTrace());
			throw ce;
		}
		catch(ChannelClosedException cce)
		{
			CException ce = new CException("sendMessage - Communication channel is already closed");
			ce.setStackTrace(cce.getStackTrace());
			throw ce;
		}
	}
	
	public ReadResult sendMessageReceive(Operation op) throws CException
	{
		try {
			if(syncOp(op.getID()))
			{
				sendMessage(op);
				return removeResultFromQueue();
			}
			else
				return new ReadResult("error", op.getID(), -1, 
						"Cannot sync operation", op.getTS());
		} catch (InterruptedException e) {
			CException ce = new CException("sendMessageReceive - No element was found in responses queue");
			ce.setStackTrace(e.getStackTrace());
			throw ce;
		}
	}
}
