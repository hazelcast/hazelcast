/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ListenerRemoveCodec;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

public abstract class ClientProxy implements DistributedObject {

    protected final String name;
    private final String serviceName;
    private volatile ClientContext context;

    protected ClientProxy(String serviceName, String name) {
        this.serviceName = serviceName;
        this.name = name;
    }

    protected final String listen(ClientMessage registrationRequest, Object partitionKey,
                                  EventHandler handler, ClientMessageDecoder responseDecoder) {
        return context.getListenerService().startListening(registrationRequest, partitionKey, handler, responseDecoder);
    }

    protected final String listen(ClientMessage registrationRequest, EventHandler handler,
                                  ClientMessageDecoder responseDecoder) {
        return context.getListenerService().startListening(registrationRequest, null, handler, responseDecoder);
    }

    protected final boolean stopListening(String registrationId, ListenerRemoveCodec listenerRemoveCodec) {
        return context.getListenerService().stopListening(registrationId, listenerRemoveCodec);
    }

    protected final ClientContext getContext() {
        return context;
    }

    protected final void setContext(ClientContext context) {
        this.context = context;
    }

    protected final HazelcastClientInstanceImpl getClient() {
        return (HazelcastClientInstanceImpl) context.getHazelcastInstance();
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
        return StringPartitioningStrategy.getPartitionKey(getName());
    }

    @Override
    public final String getServiceName() {
        return serviceName;
    }

    @Override
    public final void destroy() {

        onDestroy();
        ClientMessage clientMessage = ClientDestroyProxyCodec.encodeRequest(name, getServiceName());
        context.removeProxy(this);
        try {
            new ClientInvocation(getClient(), clientMessage).invoke().get();
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

    protected <T> T invoke(ClientMessage clientMessage, Object key) {
        final int partitionId = context.getPartitionService().getPartitionId(key);
        return invokeOnPartition(clientMessage, partitionId);
    }

    protected <T> T invokeOnPartition(ClientMessage clientMessage, int partitionId) {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, partitionId).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage, int partitionId) throws InterruptedException {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage, partitionId).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }

    protected <T> T invoke(ClientMessage clientMessage) {
        try {
            final Future future = new ClientInvocation(getClient(), clientMessage).invoke();
            return (T) future.get();
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
        if (!name.equals(that.name)) {
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
        result = 31 * result + name.hashCode();
        return result;
    }
}
