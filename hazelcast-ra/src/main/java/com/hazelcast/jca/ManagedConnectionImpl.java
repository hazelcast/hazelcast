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

import com.hazelcast.impl.ThreadContext;
import com.hazelcast.core.Hazelcast;

import javax.resource.ResourceException;
import javax.resource.spi.*;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagedConnectionImpl extends JcaBase implements ManagedConnection,
        javax.resource.cci.LocalTransaction, LocalTransaction {
    private final ConnectionImpl conn;
    private List<ConnectionEventListener> lsListeners = null;
    private PrintWriter printWriter = null;
    private static AtomicInteger idGen = new AtomicInteger();
    private transient final int id;

    public ManagedConnectionImpl() {
        conn = new ConnectionImpl(this);
        id = idGen.incrementAndGet();
    }

    public void associateConnection(Object arg0) throws ResourceException {
        log(this, "associateConnection: " + arg0);
    }

    public void cleanup() throws ResourceException {
        log(this, "cleanup");
    }

    public void destroy() throws ResourceException {
        log(this, "destroy");
    }

    public Object getConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
        log(this, "getConnection");
        return conn;
    }

    public LocalTransaction getLocalTransaction() throws ResourceException {
        log(this, "getLocalTransaction");
        return this;
    }

    public PrintWriter getLogWriter() throws ResourceException {
        return printWriter;
    }

    public void setLogWriter(PrintWriter printWriter) throws ResourceException {
        this.printWriter = printWriter;
    }

    public ManagedConnectionMetaData getMetaData() throws ResourceException {
        return null;
    }

    public XAResource getXAResource() throws ResourceException {
        log(this, "getXAResource");
        return null;
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        log(this, "addConnectionEventListener");
        if (lsListeners == null)
            lsListeners = new ArrayList<ConnectionEventListener>();
        lsListeners.add(listener);
    }

    public void removeConnectionEventListener(ConnectionEventListener listener) {
        if (lsListeners == null)
            return;
        lsListeners.remove(listener);
    }

    public void begin() throws ResourceException {
        log(this, "txn.begin");
        Hazelcast.getTransaction().begin();
        fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_STARTED);
    }

    public void commit() throws ResourceException {
        log(this, "txn.commit");
        ThreadContext.get().getTransaction().commit();
        fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
    }

    public void rollback() throws ResourceException {
        log(this, "txn.rollback");
        ThreadContext.get().getTransaction().rollback();
        fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
    }

    public void fireConnectionEvent(int event) {
        if (lsListeners == null)
            return;
        ConnectionEvent connnectionEvent = new ConnectionEvent(this, event);
        connnectionEvent.setConnectionHandle(conn);
        for (ConnectionEventListener listener : lsListeners) {
            if (event == ConnectionEvent.LOCAL_TRANSACTION_STARTED) {
                listener.localTransactionStarted(connnectionEvent);
            } else if (event == ConnectionEvent.LOCAL_TRANSACTION_COMMITTED) {
                listener.localTransactionCommitted(connnectionEvent);
            } else if (event == ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK) {
                listener.localTransactionRolledback(connnectionEvent);
            } else if (event == ConnectionEvent.CONNECTION_CLOSED) {
                listener.connectionClosed(connnectionEvent);
            }
        }
    }

    @Override
    public String toString() {
        return "hazelcast.ManagedConnectionImpl [" + id + "]";
    }
}
