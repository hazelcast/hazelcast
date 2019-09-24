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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.serialization.Data;

import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cache.CacheEventType.PARTITION_LOST;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Helper class mainly for {@link ClientCacheProxySupport} that contains utility-methods and utility-classes.
 */
final class ClientCacheProxySupportUtil {

    private ClientCacheProxySupportUtil() {
    }

    static void handleFailureOnCompletionListener(CompletionListener completionListener, Throwable t) {
        if (t instanceof Exception) {
            Throwable cause = t.getCause();
            if (t instanceof ExecutionException && cause instanceof CacheException) {
                completionListener.onException((CacheException) cause);
            } else {
                completionListener.onException((Exception) t);
            }
        } else {
            if (t instanceof OutOfMemoryError) {
                throw rethrow(t);
            } else {
                completionListener.onException(new CacheException(t));
            }
        }
    }

    static  <T> T getSafely(Future<T> future) {
        try {
            return future.get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    static <T> void addCallback(ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        if (callback == null) {
            return;
        }
        delegatingFuture.andThen(callback);
    }

    static NearCacheConfig checkNearCacheConfig(NearCacheConfig nearCacheConfig, NativeMemoryConfig nativeMemoryConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat != NATIVE) {
            return nearCacheConfig;
        }

        checkTrue(nativeMemoryConfig.isEnabled(), "Enable native memory config to use NATIVE in-memory-format "
                + "for Near Cache");
        return nearCacheConfig;
    }

    static EventHandler createHandler(CacheEventListenerAdaptor<?, ?> adaptor) {
        return new CacheEventHandler(adaptor);
    }

    static ListenerMessageCodec createCacheEntryListenerCodec(String nameWithPrefix) {
        return new CacheEntryListenerCodec(nameWithPrefix);
    }

    static ListenerMessageCodec createPartitionLostListenerCodec(String name) {
        return new PartitionLostListenerCodec(name);
    }

    static ListenerMessageCodec createInvalidationListenerCodec(String nameWithPrefix) {
        return new InvalidationListenerCodec(nameWithPrefix);
    }

    static class CacheEntryListenerCodec implements ListenerMessageCodec {

        private final String nameWithPrefix;

        CacheEntryListenerCodec(String nameWithPrefix) {
            this.nameWithPrefix = nameWithPrefix;
        }

        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CacheAddEntryListenerCodec.encodeRequest(nameWithPrefix, localOnly);
        }

        @Override
        public String decodeAddResponse(ClientMessage clientMessage) {
            return CacheAddEntryListenerCodec.decodeResponse(clientMessage).response;
        }

        @Override
        public ClientMessage encodeRemoveRequest(String realRegistrationId) {
            return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
        }
    }

    static class PartitionLostListenerCodec implements ListenerMessageCodec {

        private final String name;

        PartitionLostListenerCodec(String name) {
            this.name = name;
        }

        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CacheAddPartitionLostListenerCodec.encodeRequest(name, localOnly);
        }

        @Override
        public String decodeAddResponse(ClientMessage clientMessage) {
            return CacheAddPartitionLostListenerCodec.decodeResponse(clientMessage).response;
        }

        @Override
        public ClientMessage encodeRemoveRequest(String realRegistrationId) {
            return CacheRemovePartitionLostListenerCodec.encodeRequest(name, realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CacheRemovePartitionLostListenerCodec.decodeResponse(clientMessage).response;
        }
    }

    static class InvalidationListenerCodec implements ListenerMessageCodec {

        private final String nameWithPrefix;

        InvalidationListenerCodec(String nameWithPrefix) {
            this.nameWithPrefix = nameWithPrefix;
        }

        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CacheAddNearCacheInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
        }

        @Override
        public String decodeAddResponse(ClientMessage clientMessage) {
            return CacheAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage).response;
        }

        @Override
        public ClientMessage encodeRemoveRequest(String realRegistrationId) {
            return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
        }
    }

    static class EmptyCompletionListener
            implements CompletionListener {

        @Override
        public void onCompletion() {

        }

        @Override
        public void onException(Exception e) {

        }
    }

    static final class CacheEventHandler
            extends CacheAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final CacheEventListenerAdaptor<?, ?> adaptor;

        CacheEventHandler(CacheEventListenerAdaptor<?, ?> adaptor) {
            this.adaptor = adaptor;
        }

        @Override
        public void handleCacheEvent(int type, Collection<CacheEventData> keys, int completionId) {
            adaptor.handle(type, keys, completionId);
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    static final class FutureEntriesTuple {

        private final Future future;
        private final List<Map.Entry<Data, Data>> entries;

        FutureEntriesTuple(Future future, List<Map.Entry<Data, Data>> entries) {
            this.future = future;
            this.entries = entries;
        }

        public Future getFuture() {
            return future;
        }

        public List<Map.Entry<Data, Data>> getEntries() {
            return entries;
        }
    }

    static final class ClientCachePartitionLostEventHandler
            extends CacheAddPartitionLostListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final String name;
        private final ClientContext clientContext;
        private final CachePartitionLostListener listener;

        ClientCachePartitionLostEventHandler(String name, ClientContext clientContext, CachePartitionLostListener listener) {
            this.name = name;
            this.clientContext = clientContext;
            this.listener = listener;
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }

        @Override
        public void handleCachePartitionLostEvent(int partitionId, String uuid) {
            Member member = clientContext.getClusterService().getMember(uuid);
            listener.partitionLost(new CachePartitionLostEvent(name, member, PARTITION_LOST.getType(), partitionId));
        }
    }
}
