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
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.security.auth.Subject;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ManagedConnectionFactoryImpl implements ManagedConnectionFactory, ResourceAdapterAssociation {
	private static final long serialVersionUID = -1098455775292263655L;
	private PrintWriter printWriter = null;
    private final static AtomicInteger idGen = new AtomicInteger();
    private transient final int id;
	private ResourceAdapterImpl resourceAdapter;
    private final static ILogger logger = Logger.getLogger("com.hazelcast.jca");
    
    public ManagedConnectionFactoryImpl() {
        id = idGen.incrementAndGet();
    }

    public Object createConnectionFactory() throws ResourceException {
        return createConnectionFactory(null);
    }

    public Object createConnectionFactory(ConnectionManager cm) throws ResourceException {
    	logger.log(Level.FINEST, "createConnectionFactory cm: " + cm);
        return new ConnectionFactoryImpl(this, cm);
    }

    public ManagedConnection createManagedConnection(Subject arg0, ConnectionRequestInfo arg1)
            throws ResourceException {
    	logger.log(Level.FINEST, "createManagedConnection");
        return new ManagedConnectionImpl(getResourceAdapter());
    }

    public void setLogWriter(PrintWriter printWriter) throws ResourceException {
        this.printWriter = printWriter;
    }

    public PrintWriter getLogWriter() throws ResourceException {
        return printWriter;
    }

    @SuppressWarnings("unchecked")
	public ManagedConnection matchManagedConnections(Set set, Subject subject,
    		ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
    	logger.log(Level.FINEST, "matchManagedConnections");
    	//always get a new one
    	return null;
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
		ManagedConnectionFactoryImpl other = (ManagedConnectionFactoryImpl) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
    public String toString() {
        return "hazelcast.ManagedConnectionFactoryImpl [" + id + "]";
    }

	public ResourceAdapterImpl getResourceAdapter() {
		return resourceAdapter;
	}

	public void setResourceAdapter(ResourceAdapter resourceAdapter)
			throws ResourceException {
		assert resourceAdapter != null;
		
		if (resourceAdapter instanceof ResourceAdapterImpl) {
			this.resourceAdapter = (ResourceAdapterImpl) resourceAdapter;
		} else {
			throw new ResourceException(resourceAdapter + " is not the expected ResoruceAdapterImpl but " + resourceAdapter.getClass());
		}
		
	}
}
