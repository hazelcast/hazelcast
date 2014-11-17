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

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientDestroyRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class ClientProxy implements DistributedObject {

    private static final AtomicReferenceFieldUpdater<ClientProxy, ClientContext> CONTEXT_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientProxy.class, ClientContext.class, "context");

    private final String serviceName;

    private final String objectName;

    private volatile ClientContext context;

    protected ClientProxy(String serviceName, String objectName) {
        this.serviceName = serviceName;
        this.objectName = objectName;
    }

    protected final String listen(ClientRequest registrationRequest, Object partitionKey, EventHandler handler) {
        return context.getListenerService().listen(registrationRequest, partitionKey, handler);
    }

    protected final String listen(ClientRequest registrationRequest, EventHandler handler) {
        return context.getListenerService().listen(registrationRequest, null, handler);
    }

    protected final boolean stopListening(BaseClientRemoveListenerRequest request, String registrationId) {
        return context.getListenerService().stopListening(request, registrationId);
    }

    protected final ClientContext getContext() {
        return context;
    }

    protected final void setContext(ClientContext context) {
        this.context = context;
    }


    @Deprecated
    public final Object getId() {
        return objectName;
    }

    @Override
    public final String getName() {
        return objectName;
    }

    @Override
    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getName());
    }

    @Override
    public final String getServiceName() {
        return serviceName;
    }

    @Override
    public final void destroy() {

        onDestroy();
        ClientDestroyRequest request = new ClientDestroyRequest(objectName, getServiceName());
        context.removeProxy(this);
        try {
            context.getInvocationService().invokeOnRandomTarget(request).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    /**
     * Called when proxy is created.
     * Overriding implementations can add initialization specific logic into this method
     * like registering a listener, creating a cleanup task etc.
     */
    protected void onInitialize() {
    }

    /**
     * Called before proxy is destroyed.
     * Overriding implementations should clean/release resources created during initialization.
     */
    protected void onDestroy() {
    }

    /**
     * Called before client shutdown.
     * Overriding implementations can add shutdown specific logic here.
     */
    protected void onShutdown() {
    }

    protected <T> T invoke(ClientRequest req, Object key) {
        try {
            final Future future = getInvocationService().invokeOnKeyOwner(req, key);
            Object result = future.get();
            return toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected <T> T invokeInterruptibly(ClientRequest req, Object key) throws InterruptedException {
        try {
            final Future future = getInvocationService().invokeOnKeyOwner(req, key);
            Object result = future.get();
            return toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }

    private ClientInvocationService getInvocationService() {
        return getContext().getInvocationService();
    }

    protected <T> T invoke(ClientRequest req) {
        try {
            final Future future = getInvocationService().invokeOnRandomTarget(req);
            Object result = future.get();
            return toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected <T> T invoke(ClientRequest req, Address address) {
        try {
            final Future future = getInvocationService().invokeOnTarget(req, address);
            Object result = future.get();
            return toObject(result);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected Data toData(Object o) {
        return getContext().getSerializationService().toData(o);
    }

    protected <T> T toObject(Object data) {
        return getContext().getSerializationService().toObject(data);
    }

    protected void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }

    private String getInstanceName() {
        ClientContext ctx = context;
        if (ctx != null) {
            HazelcastInstance instance = ctx.getHazelcastInstance();
            return instance.getName();
        }
        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientProxy that = (ClientProxy) o;

        String instanceName = getInstanceName();
        if (!instanceName.equals(that.getInstanceName())) {
            return false;
        }
        if (!objectName.equals(that.objectName)) {
            return false;
        }
        if (!serviceName.equals(that.serviceName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        String instanceName = getInstanceName();
        int result = instanceName.hashCode();
        result = 31 * result + serviceName.hashCode();
        result = 31 * result + objectName.hashCode();
        return result;
    }
}
