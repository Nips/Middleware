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

import pt.fct.di.ops.IResult;
//import pt.fct.di.ops.ReadResult;

/**
 * implementors of IAsyncCallback need to make sure that any public methods
 * are threadsafe with respect to response() being called from the message
 * service.  In particular, if any shared state is referenced, making
 * response alone synchronized will not suffice.
 */
public interface IAsyncCallback 
{
//	public void setId(String id);
	
	/**
	 * @param msg response received.
	 */
	public void response(IResult msg);
}
