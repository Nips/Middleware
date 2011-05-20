package pt.fct.di.serverProxy.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import pt.fct.di.ops.IClientOperation;
import pt.fct.di.ops.IResult;
import pt.fct.di.ops.LoggableOperation;
import pt.fct.di.ops.UpdateResult;
import pt.fct.di.ops.log.ILogOperation;
import pt.fct.di.serverProxy.sync.SyncManager;
import pt.fct.di.util.SimpleCondition;

public class MessageDispatcher {

	private ServerService _service;
	private SyncManager _sync;
	
	private AtomicInteger _noOpOccurencies; //for testing proposes only.
//	protected final SimpleCondition _clientcondition = new SimpleCondition();
//	protected final SimpleCondition _servercondition = new SimpleCondition();
//   protected final SimpleCondition _serverCondition = new SimpleCondition();
	
	public MessageDispatcher(ServerService service, SyncManager manager)
	{
		this._service = service;
		this._sync = manager;
		this._noOpOccurencies = new AtomicInteger(0);
	}
	
	public void close()
	{
		System.out.println("NoOpOccurrencies: "+_noOpOccurencies.get());
	}
	
	public IResult executeClientOperation(IClientOperation op) throws ServiceException, InterruptedException
	{
//		System.out.println("SyncPhase: "+_syncPhase.get()+", GetInfoPhase: "+_getInfoPhase.get());
//		while(_syncPhase.get() || _getInfoPhase.get()) _clientcondition.await();
//		_activeClients.incrementAndGet();
		
//		System.out.print(". ");
//		System.out.print(op.toString());
//		System.out.print("Operation received... ");
//		_sync.compareVersionVectors(op.getVersionVector()); 
		IResult result = null;
		final int opType = op.getType();
		final int opId = op.getID();
		final long[] vector = op.getVersionVector();
//		System.out.println("Operation type: "+opType);
		if(opType == 1 || opType == 2)
		{
//			System.out.print("Order and Transformation phase...");
			result = _service.orderTransformExecute((LoggableOperation)op);
			if(result.getCode() > 0) result.setVersionVector(_sync.updateAndGetVector(opId, vector[opId]));
			else
			{
//				if(result.getCode() < 0 ) System.out.println("Error somewhere on the system");
				_noOpOccurencies.incrementAndGet();
				result.setVersionVector(_sync.getOwnVector());
			}
			
//			synchronized(this)
//			{
//				System.out.print("Code: "+result.getCode());
//				System.out.print(" with generated vv: ");
//				long[] vector = result.getVersionVector();
//				for(int pos = 0; pos < vector.length; pos++)
//				System.out.print(vector[pos]+", ");
//				System.out.println("]");
//			}
		}
		else
		{
			result = _service.executeReads(op); //Read or Scan operation
			result.setVersionVector(_sync.getOwnVector());
		}
		result.setOpSeq(op.getOpSeq());
		
		return result;
	}
	
	public boolean verifyVersionVector(int serverId, long[] otherVector, Map<Integer,Long> syncList) throws InterruptedException
	{
//		_syncPhase.set(true);
//		while(_activeClients.get() > 0 && _getInfoPhase.get()) _servercondition.await();
		
//		boolean updated = _sync.verifyUpDate(otherVector, syncList);
//		_sync.updateVector(serverId, otherVector);
		
//		if(updated)
//		{
//			//TODO: See if this conditions need to be swap
//			_syncPhase.set(false);
//			_clientcondition.signalAll();
//		}
//		return updated;
		return true;
	}
	
//	public boolean applyOperations(Map<Integer,List<ILogOperation>> syncMap)
//	{		
////		_syncPhase.set(true);
////		while(_activeClients.get() > 0) _clientCondition.await();
//		
//		for(Map.Entry<Integer, List<ILogOperation>> entry : syncMap.entrySet())
//		{
//			int successfulOps = 0;
//			int clientId = entry.getKey();
//			List<ILogOperation> ops = entry.getValue();
//			for(ILogOperation op : ops)
//			{
//				if(_service.applyOperation(op)) successfulOps++;
////				ReadResult res = (ReadResult)_service.executeReads(new Read(op.getID(), 10, op.getFamilyAndKey().get_value1(), op.getFamilyAndKey().get_value2(), op.getFields(), op.getConsistency()));
////				for(Map.Entry<String, String> read : res.getValues().entrySet())
////					System.out.println("Field: "+read.getKey()+" Value: "+read.getValue());
//			}
////			if(successfulOps > 0) _sync.updateClientVersion(clientId, (long)successfulOps);
//		}
//		
////		_service.logToString();
////		_syncPhase.set(false);
////		_clientcondition.signalAll();
//		return true;
//	}
	
//	public Map<Integer,List<ILogOperation>> getSyncOperations(Map<Integer,Long> requestList) throws InterruptedException
//	{
////		_getInfoPhase.set(true);
////		while(_activeClients.get() > 0 && _syncPhase.get()) _servercondition.await();
//		
//		Map<Integer,List<ILogOperation>> syncMap = _service.getSyncOperations(requestList);
//
////		_getInfoPhase.set(false);
////		_clientcondition.signalAll();
//		
//		return syncMap;
//	}
	
}
