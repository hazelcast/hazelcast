/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.ClientBlockingChannel;
import com.hazelcast.client.connection.ClientBlockingChannelPool;
import com.hazelcast.client.connection.ThreadLocalClientBlockingChannelPool;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Future;

import static com.hazelcast.client.spi.properties.ClientProperty.SUPPORT_CONNECTIONS;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Base class for client proxies.
 * <p>
 * Allows the client to proxy operations through member nodes.
 */
public abstract class ClientProxy implements DistributedObject {

    protected final String name;
    private final String serviceName;
    private final ClientContext context;
    private final SerializationService serializationService;
    protected final ClientBlockingChannelPool clientChannelPool;
    protected final boolean supportConnectionsEnabled;

    protected ClientProxy(String serviceName, String name, ClientContext context) {
        this.serviceName = serviceName;
        this.name = name;
        this.context = context;
        this.serializationService = context.getSerializationService();

        boolean smartClient = context.getClientConfig().getNetworkConfig().isSmartRouting();
        this.supportConnectionsEnabled = context.getProperties().getBoolean(SUPPORT_CONNECTIONS) && smartClient;
        //System.out.println("Support connections enabled:"+supportConnectionsEnabled);
        if (supportConnectionsEnabled) {
            this.clientChannelPool = new ThreadLocalClientBlockingChannelPool(
                    context.getPartitionService(),
                    context.getLoggingService());
        } else {
            this.clientChannelPool = null;
        }
    }

    protected final String registerListener(ListenerMessageCodec codec, EventHandler handler) {
        return getContext().getListenerService().registerListener(codec, handler);
    }

    protected final boolean deregisterListener(String registrationId) {
        return getContext().getListenerService().deregisterListener(registrationId);
    }

    // public for testing
    public final ClientContext getContext() {
        return context;
    }


    protected SerializationService getSerializationService() {
        return serializationService;
    }

    protected <T> Data toData(T o) {
        return getSerializationService().toData(o);
    }

    protected <T> T toObject(Object data) {
        return getSerializationService().toObject(data);
    }

    protected final HazelcastClientInstanceImpl getClient() {
        return (HazelcastClientInstanceImpl) getContext().getHazelcastInstance();
    }

    @Deprecated
    public final Object getId() {
        return name;
    }

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
        getContext().getProxyManager().destroyProxy(this);
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
            new ClientInvocation(getClient(), clientMessage, getName()).invoke().get();
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
        final int partitionId = getContext().getPartitionService().getPartitionId(key);
        return invokeOnPartition(clientMessage, partitionId);
    }

    protected <T> T invokeOnPartition(ClientMessage request, int partitionId) {
        if (supportConnectionsEnabled) {
            ClientBlockingChannel channel = clientChannelPool.get(partitionId);

            Throwable exception = null;
            try {
                request.setPartitionId(partitionId);
                channel.write(request);
                channel.flush();
                return (T) channel.readResponse();
            } catch (IOException e) {
                exception = e;
                throw rethrow(e);
            } finally {
                clientChannelPool.release(partitionId, channel, exception);
            }
        } else {
            try {
                final Future future = new ClientInvocation(getClient(), request, getName(), partitionId, false).invoke();
                return (T) future.get();
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }

    protected <T> T invokeOnAddress(ClientMessage clientMessage, Address address) {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, getName(), address).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage, int partitionId) throws InterruptedException {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId, true).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }

    protected <T> T invoke(ClientMessage clientMessage) {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, getName()).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
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
