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

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//import pt.fct.di.clientProxy.comm.ClientComm;
import pt.fct.di.ops.IResult;
//import pt.fct.di.ops.ReadResult;
import pt.fct.di.util.SimpleCondition;
import pt.fct.di.util.SystemProperties;

public class QuorumResponseHandler implements IAsyncCallback
{
    protected final SimpleCondition condition = new SimpleCondition();
    protected final Collection<IResult> responses;
    protected final int responseCount;
//  private IResponseResolver<T> responseResolver;
    private final long startTime;
    
    protected String _id;

    public QuorumResponseHandler(int responseCount /*, IResponseResolver<T> responseResolver*/ )
    {
    	this.responseCount = responseCount;
//    	System.out.println("responseCount: "+responseCount);
        responses = new LinkedBlockingQueue<IResult>();
//      this.responseResolver = responseResolver;
        startTime = System.currentTimeMillis();
    }
    
    public IResult get() throws TimeoutException /*, DigestMismatchException, IOException*/
    {
//        try
//        {
            long timeout = SystemProperties.getRpcTimeout() - (System.currentTimeMillis() - startTime);
            boolean success;
            try
            {
                success = condition.await(timeout, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError(ex);
            }

            if (!success)
            {
                StringBuilder sb = new StringBuilder("");
                for (IResult message : responses)
                {
                    sb.append(message.getOpSeq());
                }
                throw new TimeoutException("Operation timed out - received only " + responses.size() + " responses from " + sb.toString() + " .");
            }
//        }
//        finally
//        {
//        	ClientComm.removeRegisteredCallback(_id);
//        }

        //return responseResolver.resolve(responses);
        if(responses.isEmpty()) return null;
        return responses.iterator().next();
    }
    
    public void response(IResult message)
    {
        responses.add(message);
//      responseResolver.preprocess(message);
        if (responses.size() >= responseCount)
        {
            condition.signal();
        }
    }

//	@Override
//	public void setId(String id) {
//		_id = id;
//	}
}
