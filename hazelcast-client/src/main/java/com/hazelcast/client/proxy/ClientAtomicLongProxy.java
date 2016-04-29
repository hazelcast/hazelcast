/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongDecrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndIncrementCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongIncrementAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongSetCodec;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link IAtomicLong}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class ClientAtomicLongProxy extends PartitionSpecificClientProxy implements IAtomicLong {

    private static final ClientMessageDecoder ADD_AND_GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongAddAndGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder COMPARE_AND_SET_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongCompareAndSetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder DECREMENT_AND_GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongDecrementAndGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_ADD_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongGetAndAddCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_SET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongGetAndSetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder INCREMENT_AND_GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongIncrementAndGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_INCREMENT_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongGetAndIncrementCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder SET_ASYNC_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder ALTER_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder GET_AND_ALTER_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongGetAndAlterCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder ALTER_AND_GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongAlterAndGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder APPLY_DECODER = new ClientMessageDecoder() {
        @Override
        public <V> V decodeClientMessage(ClientMessage clientMessage) {
            return (V) AtomicLongApplyCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return AtomicLongGetCodec.decodeResponse(clientMessage).response;
        }
    };

    public ClientAtomicLongProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync(function).join();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).join();
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().join();
    }

    @Override
    public long get() {
        return getAsync().join();
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).join();
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().join();
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().join();
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).join();
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        ClientMessage request = AtomicLongAddAndGetCodec.encodeRequest(name, delta);
        return invokeOnPartitionAsync(request, ADD_AND_GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetCodec.encodeRequest(name, expect, update);
        return invokeOnPartitionAsync(request, COMPARE_AND_SET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        ClientMessage request = AtomicLongDecrementAndGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, DECREMENT_AND_GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        ClientMessage request = AtomicLongGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage request = AtomicLongGetAndAddCodec.encodeRequest(name, delta);
        return invokeOnPartitionAsync(request, GET_AND_ADD_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage request = AtomicLongGetAndSetCodec.encodeRequest(name, newValue);
        return invokeOnPartitionAsync(request, GET_AND_SET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        ClientMessage request = AtomicLongIncrementAndGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, INCREMENT_AND_GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        ClientMessage request = AtomicLongGetAndIncrementCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, GET_AND_INCREMENT_DECODER);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        ClientMessage request = AtomicLongSetCodec.encodeRequest(name, newValue);
        return invokeOnPartitionAsync(request, SET_ASYNC_DECODER);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, ALTER_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongAlterAndGetCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, ALTER_AND_GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongGetAndAlterCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, GET_AND_ALTER_DECODER);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        isNotNull(function, "function");
        ClientMessage request = AtomicLongApplyCodec.encodeRequest(name, toData(function));
        return invokeOnPartitionAsync(request, APPLY_DECODER);
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
