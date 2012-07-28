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

import java.io.Serializable;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class ResourceAdapterImpl implements ResourceAdapter, Serializable {

	private static final long serialVersionUID = -1727994229521767306L;
	private HazelcastInstance hazelcast;

	public void endpointActivation(MessageEndpointFactory arg0, ActivationSpec arg1)
            throws ResourceException {
    }

    public void endpointDeactivation(MessageEndpointFactory arg0, ActivationSpec arg1) {
    }

    public XAResource[] getXAResources(ActivationSpec[] arg0) throws ResourceException {
        return new XAResource[0];
    }

    public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
    	setHazelcast(Hazelcast.newHazelcastInstance(new XmlConfigBuilder().build()));;
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
}
