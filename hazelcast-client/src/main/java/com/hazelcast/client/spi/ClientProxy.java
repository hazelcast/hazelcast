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

package com.hazelcast.client.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @mdogan 5/16/13
 */
public abstract class ClientProxy implements DistributedObject {

    private final String serviceName;

    private final Object objectId;

    private volatile SerializationService serializationService;

    private volatile ClientClusterService clusterService;

    private volatile ClientPartitionService partitionService;

    private volatile ClientInvocationService invocationService;

    protected ClientProxy(String serviceName, Object objectId) {
        this.serviceName = serviceName;
        this.objectId = objectId;
    }

    final void setClusterService(ClientClusterService clusterService) {
        this.clusterService = clusterService;
    }

    final void setPartitionService(ClientPartitionService partitionService) {
        this.partitionService = partitionService;
    }

    final void setInvocationService(ClientInvocationService invocationService) {
        this.invocationService = invocationService;
    }

    final void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public final SerializationService getSerializationService() {
        final SerializationService ss = serializationService;
        if (ss == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return ss;
    }

    public final ClientClusterService getClusterService() {
        final ClientClusterService cs = clusterService;
        if (cs == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return cs;
    }

    public final ClientPartitionService getPartitionService() {
        final ClientPartitionService ps = partitionService;
        if (ps == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return ps;
    }

    public final ClientInvocationService getInvocationService() {
        final ClientInvocationService is = invocationService;
        if (is == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return is;
    }

    public final Object getId() {
        return objectId;
    }

    public final String getServiceName() {
        return serviceName;
    }

    public final void destroy() {
        onDestroy();
        serializationService = null;
        clusterService = null;
        partitionService = null;
        invocationService = null;
    }

    protected abstract void onDestroy();
}
