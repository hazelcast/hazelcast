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

import com.hazelcast.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.ClientDestroyRequest;
import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.util.ListenerUtil;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class ClientProxy implements DistributedObject {

    private static final AtomicReferenceFieldUpdater<ClientProxy, ClientContext> CONTEXT_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientProxy.class, ClientContext.class, "context");

    protected final String instanceName;

    private final String serviceName;

    private final String objectName;

    private volatile ClientContext context;

    protected ClientProxy(String instanceName, String serviceName, String objectName) {
        this.instanceName = instanceName;
        this.serviceName = serviceName;
        this.objectName = objectName;
    }

    protected final String listen(ClientRequest registrationRequest, Object partitionKey, EventHandler handler) {
        return ListenerUtil.listen(context, registrationRequest, partitionKey, handler);
    }

    protected final String listen(ClientRequest registrationRequest, EventHandler handler) {
        return ListenerUtil.listen(context, registrationRequest, null, handler);
    }

    protected final boolean stopListening(BaseClientRemoveListenerRequest request, String registrationId) {
        return ListenerUtil.stopListening(context, request, registrationId);
    }

    protected final ClientContext getContext() {
        final ClientContext ctx = context;
        if (ctx == null) {
            throw new DistributedObjectDestroyedException(serviceName, objectName);
        }
        return ctx;
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
        ClientContext clientContext = this.context;
        if (clientContext == null) {
            return;
        }

        // we are going to do a cas to prevent multiple/concurrent destroy calls from succeeding. Only one needs to
        // succeed, in this case the one that is able to set the context to null.
        if (!CONTEXT_UPDATER.compareAndSet(this, clientContext, null)) {
            return;
        }

        onDestroy();
        ClientDestroyRequest request = new ClientDestroyRequest(objectName, getServiceName());
        clientContext.removeProxy(this);
        try {
            clientContext.getInvocationService().invokeOnRandomTarget(request).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected void onDestroy() {
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientProxy that = (ClientProxy) o;

        if (!instanceName.equals(that.instanceName)) {
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
        int result = instanceName.hashCode();
        result = 31 * result + serviceName.hashCode();
        result = 31 * result + objectName.hashCode();
        return result;
    }
}
