package pt.fct.di.serverProxy.comm.external;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.jgroups.Address;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import pt.fct.di.ops.Put;
import pt.fct.di.ops.OpException;
import pt.fct.di.ops.Operation;
import pt.fct.di.ops.Read;
import pt.fct.di.ops.Delete;
import pt.fct.di.ops.ReadResult;
import pt.fct.di.ops.Scan;
import pt.fct.di.serverProxy.comm.CommException;

/**
 * JGroups implementation in the serverSide.
 * This class implements the methods init, close and sendMessage from the class
 * IServerComm.
 * Additionally it has a inner class, Receiver, that implements a handler to
 * treat the receiver calls from the clients.
 * 
 * @author andre_goncalves@di
 *
 */
public class JGroupsComm extends IServerComm {
	
	private JChannel _channel;
	private Address _sourceAddr;
	
	class Receiver extends ReceiverAdapter
	{
		public Receiver()
		{
		}
		
		private Operation treatMsg(ObjectInputStream in) throws IOException, 
		ClassNotFoundException, CommException
		{
			String type = in.readUTF(); //read type of a received operation
			
			if(type.equals("read")) return new Read(in);
			else if(type.equals("scan")) return new Scan(in);
			else if(type.equals("insert")) return new Put(in);
			else if(type.equals("delete")) return new Delete(in);
			else throw new CommException("Operation not recognized! This will be ignored...");
		}
		
		@Override
		public void viewAccepted(View new_view) 
		{
			System.out.println("** view: " + new_view);
		}
		
		@Override
		public void receive(org.jgroups.Message msg)
		{	
			try
			{	
				_sourceAddr = msg.getSrc();
				//System.out.println("RECEBI A MSG...!!!!!");
				//long finalTime = System.nanoTime();
				
				byte[] buffer = msg.getBuffer();
				ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
				ObjectInputStream ois = new ObjectInputStream(bais);
				
				Operation op = treatMsg(ois);
				
				ois.close();
			}
			catch(IOException ioe) {
				ioe.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (CommException e) {
				e.printStackTrace();
			}
	    }
		
		public void sendMessage(ReadResult result)
		{
			try{
				org.jgroups.Message msg = new org.jgroups.Message(_sourceAddr, null, result.toByteArray());
				_channel.send(msg);
			}
			catch(OpException oe)
			{
//				SException se = new SException("sendMessage - Communication channel is not open");
//				se.setStackTrace(oe.getStackTrace());
//				throw se;
				oe.printStackTrace();
			}
			catch(ChannelNotConnectedException cnce)
			{
//				SException se = new SException("sendMessage - Communication channel is not open");
//				se.setStackTrace(cnce.getStackTrace());
//				throw se;
				cnce.printStackTrace();
			}
			catch(ChannelClosedException cce)
			{
//				SException se = new SException("sendMessage - Communication channel is already closed");
//				se.setStackTrace(cce.getStackTrace());
//				throw se;
				cce.printStackTrace();
			}	
		}
		
		public void close()
		{
			//do nothing
		}
	}
	
	public JGroupsComm()
	{
		super();
	}
	
	public void initServer() throws CommException
	{
		
		try
		{ 
			_channel = new JChannel(getProperties().getProperty("commConfFile", 
					getDefaultConfFile()));
			_channel.setReceiver(new Receiver());
			_channel.connect("test");
		}
		catch(ChannelException ce)
		{
			CommException coe = new CommException(ce.getMessage());
			coe.setStackTrace(ce.getStackTrace());
			throw coe;
		}
	}
	
	public void cleanup()
	{
		_channel.clearChannelListeners();
		_channel.close();
	}
}
