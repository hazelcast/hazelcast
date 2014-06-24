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

import com.hazelcast.config.ConfigBuilder;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the starting point of the whole resource adapter for hazelcast.
 * The hazelcast instance is created/fetched in this class
 */
public class ResourceAdapterImpl implements ResourceAdapter, Serializable {
    /**
     * Identity generator
     */
    private static final AtomicInteger ID_GEN = new AtomicInteger();

    private static final long serialVersionUID = -1727994229521767306L;

    /**
     * The hazelcast instance itself
     */
    private transient HazelcastInstance hazelcast;
    /**
     * The configured hazelcast configuration location
     */
    private String configurationLocation;

    /**
     * Identity
     */
    private transient int id;

    public ResourceAdapterImpl() {
        setId(ID_GEN.incrementAndGet());
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapter
     * #endpointActivation(javax.resource.spi.endpoint.MessageEndpointFactory, javax.resource.spi.ActivationSpec)
     */
    public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec)
            throws ResourceException {
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapter
     * #endpointDeactivation(javax.resource.spi.endpoint.MessageEndpointFactory, javax.resource.spi.ActivationSpec)
     */
    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapter
     * #getXAResources(javax.resource.spi.ActivationSpec[])
     */
    public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
        //JBoss is fine with null, weblogic requires an empty array
        return new XAResource[0];
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapter#start(javax.resource.spi.BootstrapContext)
     */
    public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
        // Gets/creates the hazelcast instance
        ConfigBuilder config = buildConfiguration();
        setHazelcast(Hazelcast.newHazelcastInstance(config.build()));
    }

    /**
     * Creates a hazelcast configuration based on the {@link #getConfigLocation()}
     *
     * @return the created hazelcast configuration
     * @throws ResourceAdapterInternalException If there was a problem with the configuration creation
     */
    private ConfigBuilder buildConfiguration()
            throws ResourceAdapterInternalException {
        XmlConfigBuilder config;
        if (configurationLocation == null || configurationLocation.length() == 0) {
            config = new XmlConfigBuilder();
        } else {
            try {
                config = new XmlConfigBuilder(configurationLocation);
            } catch (FileNotFoundException e) {
                throw new ResourceAdapterInternalException(e.getMessage(), e);
            }
        }
        return config;
    }

    /* (non-Javadoc)
     * @see javax.resource.spi.ResourceAdapter#stop()
     */
    public void stop() {
        if (getHazelcast() != null) {
            getHazelcast().getLifecycleService().shutdown();
        }
    }

    /**
     * Sets the underlying hazelcast instance
     */
    private void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    /**
     * Provides access to the underlying hazelcast instance
     */
    HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    /**
     * Called by the container
     *
     * @param configLocation Hazelcast's configuration location
     */
    public void setConfigLocation(String configLocation) {
        this.configurationLocation = configLocation;
    }

    /**
     * @return The configured hazelcast configuration location via RAR deployment descriptor
     */
    public String getConfigLocation() {
        return configurationLocation;
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
        ResourceAdapterImpl other = (ResourceAdapterImpl) obj;
        if (id != other.id) {
            return false;
        }
        return true;
    }

    public void setId(int id) {
        this.id = id;
    }

}
