/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pt.fct.di.clientProxy.net;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//import pt.fct.di.clientProxy.comm.ClientComm;
import pt.fct.di.ops.IResult;
//import pt.fct.di.ops.ReadResult;

public class OneResponseHandler implements IAsyncCallback
{
//	private String _id;
    private IResult result_;
    private AtomicBoolean done_ = new AtomicBoolean(false);
    private Lock lock_ = new ReentrantLock();
    private Condition condition_;
    private long startTime_;

    public OneResponseHandler()
    {        
    	//System.out.println("Novo OneResponseHandler...");
        condition_ = lock_.newCondition();
        startTime_ = System.currentTimeMillis();
    }
    
//    public void setId(String id)
//    {
//    	this._id = id;
//    }
    
    public IResult get()
    {
        lock_.lock();
        try
        {
            if (!done_.get())
            {
                condition_.await();
            }
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
        finally
        {
            lock_.unlock();
        }
        return result_;
    }
    
    public boolean isDone()
    {
        return done_.get();
    }
    
    public IResult get(long timeout, TimeUnit tu) throws TimeoutException
    {
        lock_.lock();
        try
        {            
            boolean bVal = true;
            try
            {
                if ( !done_.get() )
                {
                    long overall_timeout = timeout - (System.currentTimeMillis() - startTime_);
                    if(overall_timeout > 0)
                    {
                    	//System.out.println("Bloqueie");
                        bVal = condition_.await(overall_timeout, tu);
                        //System.out.println("Desbloqueie");
                    }
                    else
                        bVal = false;
                }
            }
            catch (InterruptedException ex)
            {
            	ex.printStackTrace();
                throw new AssertionError(ex);
            }
            
            if ( !bVal && !done_.get() )
            {                
            	System.out.println("Operation timed out.");
                throw new TimeoutException("Operation timed out.");
            }
        }
        finally
        {
        	//if(result_ == null) System.out.println("Null result at OneResponseHnadler");
            lock_.unlock();
            //ClientComm.removeRegisteredCallback(_id);
        }
        //System.out.println(result_);
        return result_;
    }
     
    @Override
    public void response(IResult msg)
    {        
        try
        {
            lock_.lock();
            if ( !done_.get() )
            {     
            	//System.out.println("Recebi uma mensagem: "+msg.toString());
                result_ = msg;
                done_.set(true);
                condition_.signal();
            }
        }
        finally
        {
            lock_.unlock();
        }        
    }
}
