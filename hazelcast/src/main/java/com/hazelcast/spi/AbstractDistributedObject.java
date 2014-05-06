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

package com.hazelcast.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public abstract class AbstractDistributedObject<S extends RemoteService> implements DistributedObject {

    protected static final PartitioningStrategy PARTITIONING_STRATEGY = StringPartitioningStrategy.INSTANCE;

    private volatile NodeEngine nodeEngine;
    private volatile S service;

    protected AbstractDistributedObject(NodeEngine nodeEngine, S service) {
        this.nodeEngine = nodeEngine;
        this.service = service;
    }

    protected Data getNameAsPartitionAwareData() {
        String name = getName();
        return getNodeEngine().getSerializationService().toData(name, PARTITIONING_STRATEGY);
    }

    @Override
    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getName());
    }

    @Override
    public final void destroy() {
        NodeEngine engine = getNodeEngine();
        ProxyService proxyService = engine.getProxyService();
        proxyService.destroyDistributedObject(getServiceName(), getName());
        postDestroy();
    }

    protected void postDestroy() {

    }

    public final NodeEngine getNodeEngine() {
        final NodeEngine engine = nodeEngine;
        lifecycleCheck(engine);
        return engine;
    }

    private void lifecycleCheck(final NodeEngine engine) {
        if (engine == null || !engine.isActive()) {
            throw throwNotActiveException();
        }
    }

    protected RuntimeException throwNotActiveException() {
        throw new HazelcastInstanceNotActiveException();
    }

    public final S getService() {
        final S s = service;
        if (s == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return s;
    }

    public abstract String getServiceName();

    public final void invalidate() {
        nodeEngine = null;
        service = null;
    }

    @Override
    @Deprecated
    public final Object getId() {
        return getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DistributedObject that = (DistributedObject) o;
        Object name = getName();
        if (name != null ? !name.equals(that.getName()) : that.getName() != null) return false;

        String serviceName = getServiceName();
        if (serviceName != null ? !serviceName.equals(that.getServiceName()) : that.getServiceName() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getServiceName() != null ? getServiceName().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        return result;
    }
}
