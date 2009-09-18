/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.client;

public class ClientProxy {
	OutRunnable out ;
	protected String name = "";

	public void setOutRunnable(OutRunnable out) {
		this.out = out;
	}

	protected Packet callAndGetResult(Packet request) {
		Call c = createCall(request);
	    synchronized (c) {
			try {
				out.enQueue(c);
				c.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	    
	    Packet response = c.getResponse();
		return response;
	}
	
	protected void call(Packet request) {
		Call c = createCall(request);
		out.enQueue(c);
	}
	

	private Call createCall(Packet request) {
		Call c = new Call();
	    c.setId(Call.callIdGen.incrementAndGet());
	    c.setRequest(request);
		return c;
	}

	protected Packet createRequestPacket() {
		Packet request = new Packet();	    
	    request.setName(name);
	    request.setThreadId((int)Thread.currentThread().getId());
		return request;
	}

}
