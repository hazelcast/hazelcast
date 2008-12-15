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
 
package com.hazelcast.jca;

import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.ResultSetInfo;
import javax.resource.spi.ConnectionEvent;

public class ConnectionImpl extends JcaBase implements Connection {
	final ManagedConnectionImpl mc;
	private static AtomicInteger idGen = new AtomicInteger();
	private final int id;

	public ConnectionImpl(ManagedConnectionImpl mc) {
		super();
		this.mc = mc;
		id = idGen.incrementAndGet();
	}

	public void close() throws ResourceException {
		log(this, "close");
		mc.fireConnectionEvent(ConnectionEvent.CONNECTION_CLOSED);
	}

	public Interaction createInteraction() throws ResourceException {
		return null;
	}

	public javax.resource.cci.LocalTransaction getLocalTransaction() throws ResourceException {
		log(this, "getLocalTransaction");
		return mc;
	}

	public ConnectionMetaData getMetaData() throws ResourceException {
		return null;
	}

	public ResultSetInfo getResultSetInfo() throws ResourceException {
		return null;
	}

	@Override
	public String toString() {
		return "hazelcast.ConnectionImpl [" + id + "]";
	}
}