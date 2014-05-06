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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import com.hazelcast.core.HazelcastInstance;

public class ManagedConnectionImpl extends JcaBase implements ManagedConnection {
    /**
     * Identity generator
     */
    private static final AtomicInteger ID_GEN = new AtomicInteger();
    /**
     * Identity
     */
    private final transient int id;

    private final ManagedConnectionFactoryImpl factory;
    private final ConnectionRequestInfo cxRequestInfo;

    private final XAResourceWrapper xaResource;
    private HazelcastTransactionImpl tx;

    // Application server will always register at least one listener
    private final List<ConnectionEventListener> connectionEventListeners = new ArrayList<ConnectionEventListener>(1);

    public ManagedConnectionImpl(ConnectionRequestInfo cxRequestInfo, ManagedConnectionFactoryImpl factory) {
        this.setLogWriter(factory.getLogWriter());
        log(Level.FINEST, "ManagedConnectionImpl");

        this.factory = factory;
        this.cxRequestInfo = cxRequestInfo;

        this.id = ID_GEN.incrementAndGet();
        this.tx = new HazelcastTransactionImpl(factory, this);
        this.xaResource = new XAResourceWrapper(this);

        factory.logHzConnectionEvent(this, HzConnectionEvent.CREATE);
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        log(Level.FINEST, "addConnectionEventListener: " + listener);
        connectionEventListeners.add(listener);
    }

    public void associateConnection(Object arg0) throws ResourceException {
        log(Level.FINEST, "associateConnection: " + arg0);
    }

    public void cleanup() throws ResourceException {
        log(Level.FINEST, "cleanup");
        factory.logHzConnectionEvent(this, HzConnectionEvent.CLEANUP);
    }

    public void destroy() throws ResourceException {
        log(Level.FINEST, "destroy");
        factory.logHzConnectionEvent(this, HzConnectionEvent.DESTROY);
    }

    void fireConnectionEvent(int event) {
        fireConnectionEvent(event, null);
    }

    void fireConnectionEvent(int event, Connection conn) {
        log(Level.FINEST, "fireConnectionEvent: " + event);

        ConnectionEvent connnectionEvent = new ConnectionEvent(this, event);

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
                        //Connection handle is only required for close as per spec 6.5.7
                        connnectionEvent.setConnectionHandle(conn);
                        listener.connectionClosed(connnectionEvent);
                    }
                    break;
                default:
                    log(Level.WARNING, "Uknown event ignored: " + event);
            }
        }
    }

    public HazelcastConnection getConnection(Subject subject,
                                             ConnectionRequestInfo connectionRequestInfo) {
        log(Level.FINEST, "getConnection: " + subject + ", "
                + connectionRequestInfo);
        // must be new as per JCA spec
        return new HazelcastConnectionImpl(this, subject);
    }

    public ConnectionRequestInfo getCxRequestInfo() {
        return cxRequestInfo;
    }

    HazelcastInstance getHazelcastInstance() {
        return getResourceAdapter().getHazelcast();
    }

    public HazelcastTransaction getLocalTransaction() {
        log(Level.FINEST, "getLocalTransaction");
        return new HazelcastTransactionImpl(factory, this);
        //return tx;
    }

    public ManagedConnectionMetaData getMetaData() {
        return new ManagedConnectionMetaData();
    }

    private ResourceAdapterImpl getResourceAdapter() {
        return factory.getResourceAdapter();
    }

    public XAResource getXAResource() throws ResourceException {
        log(Level.FINEST, "getXAResource");
        // must be the same per JCA spec
        return xaResource;
    }

    public HazelcastTransactionImpl getTx() {
        return tx;
    }

    protected boolean isDeliverClosed() {
        return true;
    }

    protected boolean isDeliverCommitedEvent() {
        return true;
    }

    protected boolean isDeliverRolledback() {
        return true;
    }

    protected boolean isDeliverStartedEvent() {
        return false;
    }

    public void removeConnectionEventListener(ConnectionEventListener listener) {
        log(Level.FINEST, "removeConnectionEventListener: " + listener);
        connectionEventListeners.remove(listener);
    }

    @Override
    public String toString() {
        return "hazelcast.ManagedConnectionImpl [" + id + "]";
    }

}
