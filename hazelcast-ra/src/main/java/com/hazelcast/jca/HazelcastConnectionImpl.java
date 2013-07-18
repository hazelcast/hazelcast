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

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
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

public class HazelcastConnectionImpl implements HazelcastConnection {
	/** Reference to this creator and access to container infrastructure */
    final ManagedConnectionImpl managedConnection;
    /** Identity generator */
    private static AtomicInteger idGen = new AtomicInteger();
    /** this identity */
    private final int id;
    
    public HazelcastConnectionImpl(ManagedConnectionImpl managedConnectionImpl, Subject subject) {
        super();
        this.managedConnection = managedConnectionImpl;
        id = idGen.incrementAndGet();
    }
    
    /* (non-Javadoc)
     * @see javax.resource.cci.Connection#close()
     */
    public void close() throws ResourceException {
    	managedConnection.log(Level.FINEST, "close");
    	//important: inform the container!
        managedConnection.fireConnectionEvent(ConnectionEvent.CONNECTION_CLOSED, this);
    }

    public Interaction createInteraction() throws ResourceException {
    	//TODO
        return null;
    }

    /**
     * @throws NotSupportedException as this is not supported by this resource adapter
     */
    public ResultSetInfo getResultSetInfo() throws NotSupportedException {
    	//as per spec 15.11.3
    	throw new NotSupportedException();
    }
    
    public HazelcastTransaction getLocalTransaction() throws ResourceException {
    	managedConnection.log(Level.FINEST, "getLocalTransaction");
        return managedConnection.getLocalTransaction();
    }

    public ConnectionMetaData getMetaData() throws ResourceException {
    	return managedConnection.getMetaData();
    }

    @Override
    public String toString() {
        return "hazelcast.ConnectionImpl [" + id + "]";
    }
    
    /**
     * Method is not exposed to force all clients to go through this connection object and its
     * methods from {@link HazelcastConnection}
     * @return the local hazelcast instance
     */
    private HazelcastInstance getHazelcastInstance() {
    	return managedConnection.getHazelcastInstance();
    }
    
    /* (non-Javadoc)
     * @see com.hazelcast.jca.HazelcastConnection#getMap(java.lang.String)
     */
    public <K, V> IMap<K, V> getMap(String name) {
    	return getHazelcastInstance().getMap(name);
    }

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getQueue(java.lang.String)
	 */
	public <E> IQueue<E> getQueue(String name) {
		return getHazelcastInstance().getQueue(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getTopic(java.lang.String)
	 */
	public <E> ITopic<E> getTopic(String name) {
		return getHazelcastInstance().getTopic(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getSet(java.lang.String)
	 */
	public <E> ISet<E> getSet(String name) {
		return getHazelcastInstance().getSet(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getList(java.lang.String)
	 */
	public <E> IList<E> getList(String name) {
		return getHazelcastInstance().getList(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getMultiMap(java.lang.String)
	 */
	public <K, V> MultiMap<K, V> getMultiMap(String name) {
		return getHazelcastInstance().getMultiMap(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getExecutorService()
	 */
	public ExecutorService getExecutorService() {
		return getHazelcastInstance().getExecutorService();
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getExecutorService(java.lang.String)
	 */
	public ExecutorService getExecutorService(String name) {
		return getHazelcastInstance().getExecutorService(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getAtomicNumber(java.lang.String)
	 */
	public AtomicNumber getAtomicNumber(String name) {
		return getHazelcastInstance().getAtomicNumber(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getCountDownLatch(java.lang.String)
	 */
	public ICountDownLatch getCountDownLatch(String name) {
		return getHazelcastInstance().getCountDownLatch(name);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.jca.HazelcastConnection#getSemaphore(java.lang.String)
	 */
	public ISemaphore getSemaphore(String name) {
		return getHazelcastInstance().getSemaphore(name);
	}
}
