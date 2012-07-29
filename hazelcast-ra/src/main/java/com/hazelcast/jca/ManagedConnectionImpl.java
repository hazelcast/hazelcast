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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ManagedConnectionImpl implements ManagedConnection,
	javax.resource.cci.LocalTransaction, LocalTransaction {
	private final static ILogger logger = Logger.getLogger("com.hazelcast.jca");
	private final static AtomicInteger idGen = new AtomicInteger();
	private final ResourceAdapterImpl resourceAdapter;

	//Application server will always register at least one listener
    private final List<ConnectionEventListener> connectionEventListeners = new ArrayList<ConnectionEventListener>(1);
    private final XAResourceImpl xaResource;
    private transient final int id;

    private PrintWriter logWriter = null;

    public ManagedConnectionImpl(ResourceAdapterImpl resourceAdapter) {
    	log(Level.FINEST, "ManagedConnectionImpl");
        xaResource = new XAResourceImpl(this);
        this.resourceAdapter = resourceAdapter;
        id = idGen.incrementAndGet();
    }
    
    void log(Level logLevel, String message) {
    	log(logLevel, message, null);
    }
    
    void log(Level logLevel, String message, Throwable t) {
    	if (logger.isLoggable(logLevel)) {
	    	logger.log(logLevel, message, t);
	    	final PrintWriter logWriter = getLogWriter();
			if (logWriter != null) {
	    		logWriter.write(message);
	    		if (t != null) {
	    			t.printStackTrace(logWriter);
	    		}
	    	}
    	}
    }
    
    public void associateConnection(Object arg0) throws ResourceException {
    	log(Level.FINEST, "associateConnection: " + arg0);
    }

    public void cleanup() throws ResourceException {
    	log(Level.FINEST, "cleanup");
    }

    public void destroy() throws ResourceException {
    	log(Level.FINEST, "destroy");
    }

    public Object getConnection(Subject subject, ConnectionRequestInfo connectionRequestInfo) {
    	log(Level.FINEST, "getConnection: " + subject + ", " + connectionRequestInfo);
    	//must be new per JCA spec
        return new ConnectionImpl(this, subject);
    }

    public LocalTransaction getLocalTransaction() {
    	log(Level.FINEST, "getLocalTransaction");
        return this;
    }

    public PrintWriter getLogWriter() {
        return logWriter;
    }

    public void setLogWriter(PrintWriter printWriter) {
        this.logWriter = printWriter;
    }

    public ManagedConnectionMetaData getMetaData() {
        return new ManagedConnectionMetaData();
    }

    public XAResource getXAResource() throws ResourceException {
    	log(Level.FINEST, "getXAResource");
    	//must be the same per JCA spec
        return xaResource;
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
    	log(Level.FINEST, "addConnectionEventListener: " + listener);
        connectionEventListeners.add(listener);
    }

    public void removeConnectionEventListener(ConnectionEventListener listener) {
    	log(Level.FINEST, "removeConnectionEventListener: " + listener);
        connectionEventListeners.remove(listener);
    }

    public void begin() throws ResourceException {
    	log(Level.FINEST, "begin");
        getHazelcastInstance().getTransaction().begin();
        fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_STARTED);
    }

    public void commit() throws ResourceException {
    	log(Level.FINEST, "commit");
    	Transaction transaction = ThreadContext.get().getTransaction();
		if (transaction != null) {
    		transaction.commit();
    		fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
    	} else {
    		log(Level.WARNING, "Missed transaction commit due to thread switch!");
    	}
    }

    public void rollback() throws ResourceException {
    	log(Level.FINEST, "rollback");
    	Transaction transaction = ThreadContext.get().getTransaction();
		if (transaction != null) {
    		transaction.rollback();
    		fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
    	} else {
    		log(Level.WARNING, "Missed transaction rollback due to thread switch!");
    	}
    }

    void fireConnectionEvent(int event) {
    	fireConnectionEvent(event, null);
    }
    
    void fireConnectionEvent(int event, Connection conn) {
    	log(Level.FINEST, "fireConnectionEvevnt: " + event);

    	ConnectionEvent connnectionEvent = new ConnectionEvent(this, event);
        connnectionEvent.setConnectionHandle(conn);
        
        for (ConnectionEventListener listener : connectionEventListeners) {
        	switch (event) {
        	case ConnectionEvent.LOCAL_TRANSACTION_STARTED:
        		if (isDeliverStartedEvent()) {
        			listener.localTransactionStarted(connnectionEvent);
        		}
        		break;
        	case ConnectionEvent.LOCAL_TRANSACTION_COMMITTED:
        		if (isDeliverCommitedEvent()) {
        			listener.localTransactionCommitted(connnectionEvent);
        		} 
                break;
        	case ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK:
        		if (isDeliverRolledback()) {
        			listener.localTransactionRolledback(connnectionEvent);
        		}
                break;
        	case ConnectionEvent.CONNECTION_CLOSED:
        		if (isDeliverClosed()) {
        			listener.connectionClosed(connnectionEvent);
        		}
        		break;
        	default:
        		log(Level.WARNING, "Uknown event ignored: " + event);
        	};
        }
    }

    protected boolean isDeliverStartedEvent() {
		return false;
	}

	protected boolean isDeliverCommitedEvent() {
		return false;
	}

	protected boolean isDeliverRolledback() {
		return false;
	}

	protected boolean isAutoClose() {
		return false;
	}

	protected boolean isDeliverClosed() {
		return false;
	}

	@Override
    public String toString() {
        return "hazelcast.ManagedConnectionImpl [" + id + "]";
    }

	protected HazelcastInstance getHazelcastInstance() {
		return getResourceAdapter().getHazelcast();
	}
	
	private ResourceAdapterImpl getResourceAdapter() {
		return resourceAdapter;
	}
}
