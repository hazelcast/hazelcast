/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.*;
import javax.resource.spi.ConnectionManager;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Small facade to bring together container's pooling mechanism and other 
 * vendor-specific calls with the real implementation classes of this 
 * resource adapter
 */
public class ConnectionFactoryImpl implements HazelcastConnectionFactory {
	private static final long serialVersionUID = -5909363703528221650L;
	/** Access to this resource adapter infrastructure */
	private ManagedConnectionFactoryImpl mcf;
	/** Container's connection manager - i.e. for pooling */
    private ConnectionManager cm;
    /** JNDI reference - not used */
    private Reference ref;
    /** identity generator */
    private final static AtomicInteger idGen = new AtomicInteger();
    /** class logger*/
    private final static ILogger logger = Logger.getLogger("com.hazelcast.jca");
    /** this identity */
    private transient final int id;

    public ConnectionFactoryImpl() {
    	id = idGen.incrementAndGet();
    }
    
    public ConnectionFactoryImpl(ManagedConnectionFactoryImpl mcf, ConnectionManager cm) {
    	this();
        this.mcf = mcf;
        this.cm = cm;
    }
    
    /* (non-Javadoc)
     * @see com.hazelcast.jca.HazelcastConnectionFactory#getConnection()
     */
    public HazelcastConnection getConnection() throws ResourceException {
        logger.finest( "getConnection");
        return this.getConnection(null);
    }

    /* (non-Javadoc)
     * @see com.hazelcast.jca.HazelcastConnectionFactory#getConnection(javax.resource.cci.ConnectionSpec)
     */
    public HazelcastConnection getConnection(ConnectionSpec connSpec) throws ResourceException {
    	logger.finest( "getConnection spec: " + connSpec);
        return (HazelcastConnectionImpl) cm.allocateConnection(mcf, null);
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.ConnectionFactory#getMetaData()
     */
    public ResourceAdapterMetaData getMetaData() throws ResourceException {
        return new ConnectionFactoryMetaData();
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.ConnectionFactory#getRecordFactory()
     */
    public RecordFactory getRecordFactory() throws ResourceException {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.resource.Referenceable#setReference(javax.naming.Reference)
     */
    public void setReference(Reference ref) {
        this.ref = ref;
    }

    /* (non-Javadoc)
     * @see javax.naming.Referenceable#getReference()
     */
    public Reference getReference() throws NamingException {
        return ref;
    }

    @Override
    public String toString() {
        return "hazelcast.ConnectionFactoryImpl [" + id + "]";
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ConnectionFactoryImpl other = (ConnectionFactoryImpl) obj;
		if (id != other.id)
			return false;
		return true;
	}
    
}
