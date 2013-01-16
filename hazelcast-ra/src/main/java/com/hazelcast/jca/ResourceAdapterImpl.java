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
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import java.util.concurrent.atomic.AtomicInteger;

public class ResourceAdapterImpl implements ResourceAdapter {
    private final static AtomicInteger idGen = new AtomicInteger();
    private transient final int id;

    public ResourceAdapterImpl() {
        id = idGen.incrementAndGet();
    }

    public void endpointActivation(MessageEndpointFactory arg0, ActivationSpec arg1)
            throws ResourceException {
    }

    public void endpointDeactivation(MessageEndpointFactory arg0, ActivationSpec arg1) {
    }

    public XAResource[] getXAResources(ActivationSpec[] arg0) throws ResourceException {
        return null;
    }

    public void start(BootstrapContext arg0) throws ResourceAdapterInternalException {
    }

    public void stop() {
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || !(obj instanceof ResourceAdapterImpl)) {
            return false;
        }
        return ((ResourceAdapterImpl)obj).id == id;
    }
}
