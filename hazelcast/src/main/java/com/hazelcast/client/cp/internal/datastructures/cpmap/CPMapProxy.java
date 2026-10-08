/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures.cpmap;

import com.hazelcast.client.cp.internal.datastructures.CPClientProxy;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapSetCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import javax.annotation.Nonnull;

import static com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy.MESSAGE_EXPECTED_VALUE;
import static com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy.MESSAGE_KEY;
import static com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy.MESSAGE_NEW_VALUE;
import static com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy.MESSAGE_VALUE;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Client proxy implementation for {@link CPMap}.
 * @param <K> Key type
 * @param <V> Value type
 */
public class CPMapProxy<K, V> extends CPClientProxy implements CPMap<K, V> {
    private enum KeyOp {
        GET,
        REMOVE,
        DELETE
    }

    private enum KeyValueOp {
        PUT,
        PUT_IF_ABSENT,
        SET
    }

    public CPMapProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(CPMapService.SERVICE_NAME, proxyName, context, groupId, objectName);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        return keyValueRequest(KeyValueOp.PUT, key, value);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value) {
        return keyValueRequest(KeyValueOp.PUT_IF_ABSENT, key, value);
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value) {
        keyValueRequest(KeyValueOp.SET, key, value);
    }

    @Override
    public V remove(@Nonnull K key) {
        return removeRequest(key);
    }

    @Override
    public void delete(@Nonnull K key) {
        deleteRequest(key);
    }

    @Override
    public boolean compareAndSet(@Nonnull K key, @Nonnull V expectedValue, @Nonnull V newValue) {
        checkNotNull(key, MESSAGE_KEY);
        checkNotNull(expectedValue, MESSAGE_EXPECTED_VALUE);
        checkNotNull(newValue, MESSAGE_NEW_VALUE);
        SerializationService serializationService = getContext().getSerializationService();
        Data dataKey = serializationService.toData(key);
        Data dataExpectedValue = serializationService.toData(expectedValue);
        Data dataNewValue = serializationService.toData(newValue);
        ClientMessage request =
                CPMapCompareAndSetCodec.encodeRequest(groupId, objectName, dataKey, dataExpectedValue, dataNewValue);
        ClientInvocationFuture f = invokeClientRequest(request, name);
        return new ClientDelegatingFuture<Boolean>(
                f,
                getSerializationService(),
                CPMapCompareAndSetCodec::decodeResponse).joinInternal();
    }

    @Override
    public V get(@Nonnull K key) {
        return getRequest(key);
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        invokeClientRequest(request, name).joinInternal();
    }

    private V keyValueRequest(KeyValueOp keyValueOp, K key, V value) {
        checkNotNull(key, MESSAGE_KEY);
        checkNotNull(value, MESSAGE_VALUE);
        SerializationService serializationService = getContext().getSerializationService();
        Data dataKey = serializationService.toData(key);
        Data dataValue = serializationService.toData(value);
        ClientMessage request = createKeyValueClientMessage(keyValueOp, dataKey, dataValue);
        ClientInvocationFuture f = invokeClientRequest(request, name);
        if (KeyValueOp.SET == keyValueOp) {
            f.joinInternal();
            return null;
        }
        return new ClientDelegatingFuture<V>(
                f,
                getSerializationService(),
                KeyValueOp.PUT == keyValueOp ? CPMapPutCodec::decodeResponse : CPMapPutIfAbsentCodec::decodeResponse
        ).joinInternal();
    }

    private ClientMessage createKeyValueClientMessage(KeyValueOp keyValueOp, Data key, Data value) {
        ClientMessage message;
        switch (keyValueOp) {
            case SET:
                message = CPMapSetCodec.encodeRequest(groupId, objectName, key, value);
                break;
            case PUT:
                message = CPMapPutCodec.encodeRequest(groupId, objectName, key, value);
                break;
            case PUT_IF_ABSENT:
                message = CPMapPutIfAbsentCodec.encodeRequest(groupId, objectName, key, value);
                break;
            default:
                throw new IllegalArgumentException("Unknown op: " + keyValueOp);
        }
        return message;
    }

    private V getRequest(K key) {
        return keyRequest(key, KeyOp.GET);
    }

    private V removeRequest(K key) {
        return keyRequest(key, KeyOp.REMOVE);
    }

    private void deleteRequest(K key) {
        keyRequest(key, KeyOp.DELETE);
    }

    private V keyRequest(K key, KeyOp keyOp) {
        checkNotNull(key, MESSAGE_KEY);
        Data dataKey = getContext().getSerializationService().toData(key);
        ClientMessage request = getKeyClientMessage(keyOp, dataKey);
        ClientInvocationFuture clientInvocationFuture = invokeClientRequest(request, name);
        if (KeyOp.DELETE == keyOp) {
            clientInvocationFuture.joinInternal();
            return null;
        }

        ClientDelegatingFuture<V> clientDelegatingFuture =
                new ClientDelegatingFuture<>(
                        clientInvocationFuture,
                        getSerializationService(),
                        keyOp == KeyOp.GET ? CPMapGetCodec::decodeResponse : CPMapRemoveCodec::decodeResponse);
        return clientDelegatingFuture.joinInternal();
    }

    private ClientMessage getKeyClientMessage(KeyOp keyOp, Data key) {
        switch (keyOp) {
            case GET:
                return CPMapGetCodec.encodeRequest(groupId, objectName, key);
            case REMOVE:
                return CPMapRemoveCodec.encodeRequest(groupId, objectName, key);
            case DELETE:
                return CPMapDeleteCodec.encodeRequest(groupId, objectName, key);
            default:
                throw new IllegalArgumentException("Unknown op: " + keyOp);
        }
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }
}
