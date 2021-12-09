/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec;
import com.hazelcast.client.impl.spi.invocation.ClientInvocationFuture;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Base class for client proxies.
 * <p>
 * Allows the client to proxy operations through member nodes.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class ClientProxy implements DistributedObject {

    protected final String name;

    private final String serviceName;
    private final ClientContext context;
    private final SerializationService serializationService;

    protected ClientProxy(String serviceName, String name, ClientContext context) {
        this.serviceName = serviceName;
        this.name = name;
        this.context = context;
        this.serializationService = context.getSerializationService();
    }

    protected final @Nonnull
    UUID registerListener(ListenerMessageCodec codec, EventHandler handler) {
        return context.getListenerService().registerListener(codec, handler);
    }

    protected final boolean deregisterListener(@Nonnull UUID registrationId) {
        return context.getListenerService().deregisterListener(registrationId);
    }

    // public for testing
    public final ClientContext getContext() {
        return context;
    }

    protected SerializationService getSerializationService() {
        return serializationService;
    }

    protected <T> Data toData(T o) {
        return serializationService.toData(o);
    }

    protected <T> T toObject(Object data) {
        return serializationService.toObject(data);
    }

    @Nonnull
    @Override
    public final String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getDistributedObjectName());
    }

    @Override
    public final String getServiceName() {
        return serviceName;
    }

    protected String getDistributedObjectName() {
        return name;
    }

    @Override
    public final void destroy() {
        context.getProxyManager().destroyProxy(this);
    }

    /**
     * Destroys this client proxy instance locally without issuing distributed
     * object destroy request to the cluster as the {@link #destroy} method
     * does.
     * <p>
     * The local destruction operation still may perform some communication
     * with the cluster; for example, to unregister remote event subscriptions.
     */
    public final void destroyLocally() {
        if (preDestroy()) {
            try {
                onDestroy();
            } finally {
                postDestroy();
            }
        }
    }

    /**
     * Destroys the remote distributed object counterpart of this proxy by
     * issuing the destruction request to the cluster.
     */
    public final void destroyRemotely() {
        ClientMessage clientMessage = ClientDestroyProxyCodec.encodeRequest(getDistributedObjectName(), getServiceName());
        try {
            context.getInvocationService().invokeOnRandom(clientMessage, getName()).get();
        } catch (Exception e) {
            throw rethrow(e);
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
     * Called before proxy is destroyed and determines whether destroy should be done.
     *
     * @return <code>true</code> if destroy should be done, otherwise <code>false</code>
     */
    protected boolean preDestroy() {
        return true;
    }

    /**
     * Called after proxy is destroyed.
     */
    protected void postDestroy() {
    }

    /**
     * Called before client shutdown.
     * Overriding implementations can add shutdown specific logic here.
     */
    protected void onShutdown() {
    }

    protected <T> T invoke(ClientMessage clientMessage, Object key) {
        final int partitionId = context.getPartitionService().getPartitionId(key);
        return invokeOnPartition(clientMessage, partitionId);
    }

    protected <T> T invokeOnPartition(ClientMessage clientMessage, int partitionId) {
        try {
            Future future = context.getInvocationService().invokeOnPartition(clientMessage, getName(), partitionId);
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected ClientInvocationFuture invokeOnMemberAsync(ClientMessage clientMessage, UUID uuid) {
        return context.getInvocationService().invokeOnMember(clientMessage, getName(), uuid);
    }

    protected ClientInvocationFuture invokeOnKeyAsync(ClientMessage clientMessage, Object key) {
        final int partitionId = context.getPartitionService().getPartitionId(key);
        return invokeOnPartitionAsync(clientMessage, partitionId);
    }

    protected ClientInvocationFuture invokeOnPartitionAsync(ClientMessage clientMessage, int partitionId) {
        return context.getInvocationService().invokeOnPartition(clientMessage, getName(), partitionId);
    }

    protected <T> T invokeOnMember(ClientMessage clientMessage, UUID uuid) {
        try {
            Future future = context.getInvocationService().invokeOnMember(clientMessage, getName(), uuid);
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage, int partitionId) throws InterruptedException {
        try {
            Future future = context.getInvocationService().invokeOnPartition(clientMessage, getName(), partitionId);
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }

    protected <T> T invoke(ClientMessage clientMessage) {
        try {
            Future future = context.getInvocationService().invokeOnRandom(clientMessage, getName());
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected ClientInvocationFuture invokeAsync(ClientMessage clientMessage) {
        return context.getInvocationService().invokeOnRandom(clientMessage, getName());
    }

    protected ClientInvocationFuture invokeAsync(ClientMessage clientMessage, String name) {
        return context.getInvocationService().invokeOnRandom(clientMessage, name);
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
        if (!getInstanceName().equals(that.getInstanceName())) {
            return false;
        }
        if (!getDistributedObjectName().equals(that.getDistributedObjectName())) {
            return false;
        }
        if (!serviceName.equals(that.serviceName)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = getInstanceName().hashCode();
        result = 31 * result + serviceName.hashCode();
        result = 31 * result + getDistributedObjectName().hashCode();
        return result;
    }

    private String getInstanceName() {
        ClientContext context = getContext();
        return context != null ? context.getHazelcastInstance().getName() : "";
    }
}
