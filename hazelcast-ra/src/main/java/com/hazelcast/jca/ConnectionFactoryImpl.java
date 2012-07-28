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

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.*;
import javax.resource.spi.ConnectionManager;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ConnectionFactoryImpl implements ConnectionFactory {
	private static final long serialVersionUID = -5909363703528221650L;
	
	private final ManagedConnectionFactoryImpl mcf;
    private final ConnectionManager cm;
    private Reference ref;
    private final static AtomicInteger idGen = new AtomicInteger();
    private final static ILogger logger = Logger.getLogger("com.hazelcast.jca");
    private transient final int id;

    public ConnectionFactoryImpl(ManagedConnectionFactoryImpl mcf, ConnectionManager cm) {
        super();
        this.mcf = mcf;
        this.cm = cm;
        id = idGen.incrementAndGet();
    }
    
    
    public Connection getConnection() throws ResourceException {
        logger.log(Level.FINEST, "getConnection");
        return (Connection) cm.allocateConnection(mcf, null);
    }

    public Connection getConnection(ConnectionSpec connSpec) throws ResourceException {
    	logger.log(Level.FINEST, "getConnection spec: " + connSpec);
        return (Connection) cm.allocateConnection(mcf, null);
    }

    public ResourceAdapterMetaData getMetaData() throws ResourceException {
        return new ConnectionFactoryMetaData();
    }

    public RecordFactory getRecordFactory() throws ResourceException {
        return null;
    }

    public void setReference(Reference ref) {
        this.ref = ref;
    }

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
