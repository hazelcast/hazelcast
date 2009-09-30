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

import java.util.concurrent.atomic.AtomicLong;


public class Call {
	
	public Call() {
		this.id = Call.callIdGen.incrementAndGet();
	}
	
	private Packet request;
	
	private long id;
	
	private Packet response;

	private Exception exception;
	
	public static AtomicLong callIdGen = new AtomicLong(0);
	
	public Packet getRequest() {
		return request;
	}
	public void setRequest(Packet request) {
		this.request = request;
		request.setCallId(id);
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
		if (request!=null)
			request.setCallId(id);
	}
	public Packet getResponse() {
		return response;
	}
	public void setResponse(Packet response) {
		this.response = response;
	}
	public void setException(Exception exception) {
		this.exception = exception;
	}
	public Exception getException() {
		return exception;
	}
	

}
