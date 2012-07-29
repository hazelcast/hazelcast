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

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;

import com.hazelcast.config.ConfigBuilder;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class ResourceAdapterImpl implements ResourceAdapter, Serializable {

	private static final long serialVersionUID = -1727994229521767306L;
	private HazelcastInstance hazelcast;
	private String configurationLocation;

	public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec)
            throws ResourceException {
    }

    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
    }

    public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
    	//JBoss is fine with null, weblogic requires an empty array
        return new XAResource[0];
    }

    public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
    	ConfigBuilder config = buildConfiguration();
    	setHazelcast(Hazelcast.newHazelcastInstance(config.build()));;
    }

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

    public void stop() {
    	if (getHazelcast() != null) {
    		getHazelcast().getLifecycleService().shutdown();
    	}
    }

	private void setHazelcast(HazelcastInstance hazelcast) {
		this.hazelcast = hazelcast;
	}

	HazelcastInstance getHazelcast() {
		return hazelcast;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hazelcast == null) ? 0 : hazelcast.hashCode());
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
		ResourceAdapterImpl other = (ResourceAdapterImpl) obj;
		if (hazelcast == null) {
			if (other.hazelcast != null)
				return false;
		} else if (!hazelcast.equals(other.hazelcast))
			return false;
		return true;
	}

	public void setConfigLocation(String configLocation) {
		this.configurationLocation = configLocation;
	}

	public String getConfigLocation() {
		return configurationLocation;
	}
	
}
