/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
 */

package com.hazelcast.jca;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.ResultSetInfo;
import javax.resource.spi.ConnectionEvent;
import javax.security.auth.Subject;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ConnectionImpl implements HazelcastConnection {
    final ManagedConnectionImpl managedConnection;
    private static AtomicInteger idGen = new AtomicInteger();
    private final int id;
    
    public ConnectionImpl(ManagedConnectionImpl managedConnectionImpl, Subject subject) {
        super();
        this.managedConnection = managedConnectionImpl;
        id = idGen.incrementAndGet();
    }
    
    public void close() throws ResourceException {
    	managedConnection.log(Level.FINEST, "close");
        managedConnection.fireConnectionEvent(ConnectionEvent.CONNECTION_CLOSED, this);
    }

    public Interaction createInteraction() throws ResourceException {
    	//TODO
        return null;
    }

    public ResultSetInfo getResultSetInfo() throws ResourceException {
    	//TODO
        return null;
    }
    
    public javax.resource.cci.LocalTransaction getLocalTransaction() throws ResourceException {
    	managedConnection.log(Level.FINEST, "getLocalTransaction");
        return managedConnection;
    }

    public ConnectionMetaData getMetaData() throws ResourceException {
    	return managedConnection.getMetaData();
    }


    @Override
    public String toString() {
        return "hazelcast.ConnectionImpl [" + id + "]";
    }
    
    private HazelcastInstance getHazelcastInstance() {
    	return managedConnection.getHazelcastInstance();
    }
    
    public <K, V> IMap<K, V> getMap(String name) {
    	return getHazelcastInstance().getMap(name);
    }

	public <E> IQueue<E> getQueue(String name) {
		return getHazelcastInstance().getQueue(name);
	}

	public <E> ITopic<E> getTopic(String name) {
		return getHazelcastInstance().getTopic(name);
	}

	public <E> ISet<E> getSet(String name) {
		return getHazelcastInstance().getSet(name);
	}

	public <E> IList<E> getList(String name) {
		return getHazelcastInstance().getList(name);
	}

	public <K, V> MultiMap<K, V> getMultiMap(String name) {
		return getHazelcastInstance().getMultiMap(name);
	}

	public ExecutorService getExecutorService() {
		return getHazelcastInstance().getExecutorService();
	}

	public ExecutorService getExecutorService(String name) {
		return getHazelcastInstance().getExecutorService(name);
	}

	public AtomicNumber getAtomicNumber(String name) {
		return getHazelcastInstance().getAtomicNumber(name);
	}

	public ICountDownLatch getCountDownLatch(String name) {
		return getHazelcastInstance().getCountDownLatch(name);
	}

	public ISemaphore getSemaphore(String name) {
		return getHazelcastInstance().getSemaphore(name);
	}
}
