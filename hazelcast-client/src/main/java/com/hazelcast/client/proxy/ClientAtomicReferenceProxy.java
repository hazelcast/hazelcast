/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceClearCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceIsNullCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicReferenceSetCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link IAtomicReference}.
 *
 * @param <E> type of referenced object
 */
public class ClientAtomicReferenceProxy<E> extends PartitionSpecificClientProxy implements IAtomicReference<E> {

    private static final ClientMessageDecoder APPLY_DECODER = new ClientMessageDecoder() {
        @Override
        public <R> R decodeClientMessage(ClientMessage clientMessage) {
            return (R) AtomicReferenceApplyCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder ALTER_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder ALTER_AND_GET_DECODER = new ClientMessageDecoder() {
        @Override
        public <E> E decodeClientMessage(ClientMessage clientMessage) {
            return (E) AtomicReferenceAlterAndGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_ALTER_DECODER = new ClientMessageDecoder() {
        @Override
        public <E> E decodeClientMessage(ClientMessage clientMessage) {
            return (E) AtomicReferenceGetAndAlterCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder COMPARE_AND_SET_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return AtomicReferenceCompareAndSetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder CONTAINS_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return AtomicReferenceContainsCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_DECODER = new ClientMessageDecoder() {
        @Override
        public <E> E decodeClientMessage(ClientMessage clientMessage) {
            return (E) AtomicReferenceGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder SET_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder CLEAR_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder GET_AND_SET_DECODER = new ClientMessageDecoder() {
        @Override
        public <E> E decodeClientMessage(ClientMessage clientMessage) {
            return (E) AtomicReferenceGetAndSetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder IS_NULL_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return AtomicReferenceIsNullCodec.decodeResponse(clientMessage).response;
        }
    };

    public ClientAtomicReferenceProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<E, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceApplyCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, APPLY_DECODER);
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, ALTER_DECODER);
    }

    @Override
    public void alter(IFunction<E, E> function) {
        alterAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<E> alterAndGetAsync(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceAlterAndGetCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, ALTER_AND_GET_DECODER);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<E> getAndAlterAsync(IFunction<E, E> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicReferenceGetAndAlterCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, GET_AND_ALTER_DECODER);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(E expect, E update) {
        ClientMessage request = AtomicReferenceCompareAndSetCodec.encodeRequest(name, toData(expect), toData(update));
        return invokeOnPartitionAsync(request, COMPARE_AND_SET_DECODER);
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> containsAsync(E expected) {
        ClientMessage request = AtomicReferenceContainsCodec.encodeRequest(name, toData(expected));
        return invokeOnPartitionAsync(request, CONTAINS_DECODER);
    }

    @Override
    public boolean contains(E expected) {
        return containsAsync(expected).join();
    }

    @Override
    public InternalCompletableFuture<E> getAsync() {
        ClientMessage request = AtomicReferenceGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, GET_DECODER);
    }

    @Override
    public E get() {
        return getAsync().join();
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(E newValue) {
        ClientMessage request = AtomicReferenceSetCodec.encodeRequest(name, toData(newValue));
        return invokeOnPartitionAsync(request, SET_DECODER);
    }

    @Override
    public void set(E newValue) {
        setAsync(newValue).join();
    }

    @Override
    public InternalCompletableFuture<Void> clearAsync() {
        ClientMessage request = AtomicReferenceClearCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, CLEAR_DECODER);
    }

    @Override
    public void clear() {
        clearAsync().join();
    }

    @Override
    public InternalCompletableFuture<E> getAndSetAsync(E newValue) {
        ClientMessage request = AtomicReferenceGetAndSetCodec.encodeRequest(name, toData(newValue));
        return invokeOnPartitionAsync(request, GET_AND_SET_DECODER);
    }

    @Override
    public E getAndSet(E newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public E setAndGet(E update) {
        ClientMessage request = AtomicReferenceSetAndGetCodec.encodeRequest(name, toData(update));
        ClientMessage response = invokeOnPartition(request);
        return toObject(AtomicReferenceSetAndGetCodec.decodeResponse(response).response);
    }

    @Override
    public InternalCompletableFuture<Boolean> isNullAsync() {
        ClientMessage request = AtomicReferenceIsNullCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, IS_NULL_DECODER);
    }

    @Override
    public boolean isNull() {
        return isNullAsync().join();
    }

    @Override
    public String toString() {
        return "IAtomicReference{" + "name='" + name + '\'' + '}';
    }

}

