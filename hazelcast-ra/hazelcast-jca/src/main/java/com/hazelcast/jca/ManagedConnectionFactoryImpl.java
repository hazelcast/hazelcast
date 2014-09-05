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

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.security.auth.Subject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static java.util.Collections.emptySet;

/**
 * This managed connection factory is populated with all
 * container-specific configuration and infrastructure
 *
 * @see #setConnectionTracingDetail(boolean)
 * @see #setConnectionTracingEvents(String)
 * @see #setResourceAdapter(ResourceAdapter)
 */
public class ManagedConnectionFactoryImpl extends JcaBase implements ManagedConnectionFactory, ResourceAdapterAssociation {
    private static final long serialVersionUID = -4889598421534961926L;
    /**
     * Identity generator
     */
    private static final AtomicInteger ID_GEN = new AtomicInteger();

    /**
     * Identity
     */
    private transient int id;

    /**
     * Access to this resource adapter instance itself
     */
    private ResourceAdapterImpl resourceAdapter;
    /**
     * Definies which events should be traced
     *
     * @see HzConnectionEvent
     */
    private Set<HzConnectionEvent> hzConnectionTracingEvents;

    /**
     * Should connection events be traced or not
     *
     * @see #hzConnectionTracingEvents
     */
    private boolean connectionTracingDetail;

    public ManagedConnectionFactoryImpl() {
        setId(ID_GEN.incrementAndGet());
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory()
     */
    public HazelcastConnectionFactory createConnectionFactory() throws ResourceException {
        return createConnectionFactory(null);
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionFactory#createConnectionFactory(javax.resource.spi.ConnectionManager)
     */
    public HazelcastConnectionFactory createConnectionFactory(ConnectionManager cm)
            throws ResourceException {
        log(Level.FINEST, "createConnectionFactory cm: " + cm);
        logHzConnectionEvent(this, HzConnectionEvent.FACTORY_INIT);
        return new ConnectionFactoryImpl(this, cm);
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionFactory
     * #createManagedConnection(javax.security.auth.Subject, javax.resource.spi.ConnectionRequestInfo)
     */
    public ManagedConnection createManagedConnection(Subject subject,
                                                     ConnectionRequestInfo cxRequestInfo) throws ResourceException {
        log(Level.FINEST, "createManagedConnection");

        return new ManagedConnectionImpl(cxRequestInfo, this);
    }

    /**
     * Getter for the RAR property 'connectionTracingEvents'
     */
    public String getConnectionTracingEvents() {
        return this.hzConnectionTracingEvents.toString();
    }

    /**
     * Setter for the RAR property 'connectionTracingDetails'.
     * This method is called by the container
     */
    public void setConnectionTracingDetail(boolean connectionTracingDetail) {
        this.connectionTracingDetail = connectionTracingDetail;
    }

    /**
     * Getter for the RAR property 'connectionTracingDetails'
     */
    public boolean isConnectionTracingDetail() {
        return connectionTracingDetail;
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapterAssociation#getResourceAdapter()
     */
    public ResourceAdapterImpl getResourceAdapter() {
        return resourceAdapter;
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapterAssociation#setResourceAdapter(javax.resource.spi.ResourceAdapter)
     */
    public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
        assert resourceAdapter != null;

        if (resourceAdapter instanceof ResourceAdapterImpl) {
            this.resourceAdapter = (ResourceAdapterImpl) resourceAdapter;
        } else {
            throw new ResourceException(resourceAdapter
                    + " is not the expected ResoruceAdapterImpl but "
                    + resourceAdapter.getClass());
        }
    }

    /**
     * Logs (or silently drops) the provided connection event if configured so
     *
     * @param eventSource The source from where the event originates
     * @param event       The event to log
     */
    void logHzConnectionEvent(Object eventSource, HzConnectionEvent event) {
        if (hzConnectionTracingEvents.contains(event)) {
            StringBuilder sb = new StringBuilder("HZ Connection Event <<");
            sb.append(event).append(">> for ").append(eventSource);
            sb.append(" in thread [").append(Thread.currentThread().getName());
            sb.append("]");

            if (isConnectionTracingDetail()) {
                log(Level.INFO, sb.toString());
            } else {
                //
                log(Level.INFO, sb.toString(),
                        new Exception("Hz Connection Event Call Stack"));
            }
        }
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ManagedConnectionFactory
     * #matchManagedConnections(java.util.Set, javax.security.auth.Subject, javax.resource.spi.ConnectionRequestInfo)
     */
    public ManagedConnection matchManagedConnections(@SuppressWarnings("rawtypes") Set connectionSet,
                                                     Subject subject, ConnectionRequestInfo cxRequestInfo)
            throws ResourceException {
        log(Level.FINEST, "matchManagedConnections");

        if ((null != connectionSet) && !connectionSet.isEmpty()) {
            for (Object con : (Set<?>) connectionSet) {
                if (con instanceof ManagedConnectionImpl) {
                    ManagedConnectionImpl conn = (ManagedConnectionImpl) con;
                    ConnectionRequestInfo otherCxRequestInfo = conn.getCxRequestInfo();
                    if (null == otherCxRequestInfo || otherCxRequestInfo.equals(cxRequestInfo)) {
                        return conn;
                    }

                } else {
                    log(Level.WARNING, "Unexpected element in list: " + con);
                }
            }
        }
        // null is interpreted as "create new one" by the container
        return null;
    }


    /**
     * Setter for the property 'connectionTracingEvents'.
     * This method is called by the container
     *
     * @param tracingSpec A comma delimited list of {@link HzConnectionEvent}
     */
    public void setConnectionTracingEvents(String tracingSpec) {
        if ((null != tracingSpec) && (0 < tracingSpec.length())) {
            List<HzConnectionEvent> traceEvents = new ArrayList<HzConnectionEvent>();

            for (String traceEventId : tracingSpec.split(",")) {
                traceEventId = traceEventId.trim();
                try {
                    HzConnectionEvent traceEvent = HzConnectionEvent.valueOf(traceEventId);
                    if (null != traceEvent) {
                        traceEvents.add(traceEvent);
                    }
                } catch (IllegalArgumentException iae) {
                    log(Level.WARNING, "Ignoring illegal token \""
                            + traceEventId
                            + "\" from connection config-property "
                            + "connectionTracingEvents, valid tokens are "
                            + EnumSet.allOf(HzConnectionEvent.class));
                }
            }

            this.hzConnectionTracingEvents = EnumSet.copyOf(traceEvents);
        } else {
            this.hzConnectionTracingEvents = emptySet();
        }
    }

    @Override
    public String toString() {
        return "hazelcast.ManagedConnectionFactoryImpl [" + id + "]";
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
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ManagedConnectionFactoryImpl other = (ManagedConnectionFactoryImpl) obj;
        if (id != other.id) {
            return false;
        }
        return true;
    }

    public void setId(int id) {
        this.id = id;
    }


}
