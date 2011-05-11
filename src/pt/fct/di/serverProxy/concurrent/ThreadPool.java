package pt.fct.di.serverProxy.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
	
	ExecutorService pool = null;
	
	public ThreadPool (int minThreads, int maxThreads, long timeToLive, int messageQueueSize)
	{
		pool = new ThreadPoolExecutor(minThreads, maxThreads, timeToLive, TimeUnit.MILLISECONDS, 
				new LinkedBlockingQueue<Runnable>(messageQueueSize));
	}
	
	public void executeWork(Runnable command)
	{
		pool.execute(command);
	}
	
	public void destroy()
	{
	   pool.shutdown(); // Disable new tasks from being submitted
	   try {
	     // Wait a while for existing tasks to terminate
	     if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
	       pool.shutdownNow(); // Cancel currently executing tasks
	       // Wait a while for tasks to respond to being cancelled
	       if (!pool.awaitTermination(60, TimeUnit.SECONDS))
	           System.err.println("Pool did not terminate");
	     }
	   } catch (InterruptedException ie) {
	     // (Re-)Cancel if current thread also interrupted
	     pool.shutdownNow();
	     // Preserve interrupt status
	     Thread.currentThread().interrupt();
	   }

	}
}
