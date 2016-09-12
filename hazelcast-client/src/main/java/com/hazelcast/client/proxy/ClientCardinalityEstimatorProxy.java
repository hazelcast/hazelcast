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
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAggregateAllAndEstimateCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAggregateAllCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAggregateAndEstimateCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAggregateCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec;
import com.hazelcast.core.ICardinalityEstimator;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.HashUtil;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link ICardinalityEstimator}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class ClientCardinalityEstimatorProxy
        extends PartitionSpecificClientProxy implements ICardinalityEstimator {

    private static final ClientMessageDecoder AGGREGATE_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return CardinalityEstimatorAggregateCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder AGGREGATE_AND_ESTIMATE_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CardinalityEstimatorAggregateAndEstimateCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder AGGREGATE_ALL_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return CardinalityEstimatorAggregateAllCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder AGGREGATE_ALL_AND_ESTIMATE_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CardinalityEstimatorAggregateAllAndEstimateCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder ESTIMATE_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CardinalityEstimatorEstimateCodec.decodeResponse(clientMessage).response;
        }
    };

    public ClientCardinalityEstimatorProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Override
    public String toString() {
        return "ICardinalityEstimator{" + "name='" + name + '\'' + '}';
    }

    @Override
    public boolean aggregate(long hash) {
        return aggregateAsync(hash).join();
    }

    @Override
    public boolean aggregateAll(long[] hashes) {
        return aggregateAllAsync(hashes).join();
    }

    @Override
    public long aggregateAndEstimate(long hash) {
        return aggregateAndEstimateAsync(hash).join();
    }

    @Override
    public long aggregateAllAndEstimate(long[] hashes) {
        return aggregateAllAndEstimateAsync(hashes).join();
    }

    @Override
    public boolean aggregateString(String value) {
        return aggregateStringAsync(value).join();
    }

    @Override
    public boolean aggregateAllStrings(String[] values) {
        return aggregateAllStringsAsync(values).join();
    }

    @Override
    public long estimate() {
        return estimateAsync().join();
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateAsync(long hash) {
        ClientMessage request = CardinalityEstimatorAggregateCodec.encodeRequest(name, hash);
        return invokeOnPartitionAsync(request, AGGREGATE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateAllAsync(long[] hashes) {
        ClientMessage request = CardinalityEstimatorAggregateAllCodec.encodeRequest(name, hashes);
        return invokeOnPartitionAsync(request, AGGREGATE_ALL_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> aggregateAndEstimateAsync(long hash) {
        ClientMessage request = CardinalityEstimatorAggregateAndEstimateCodec.encodeRequest(name, hash);
        return invokeOnPartitionAsync(request, AGGREGATE_AND_ESTIMATE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> aggregateAllAndEstimateAsync(long[] hashes) {
        ClientMessage request = CardinalityEstimatorAggregateAllAndEstimateCodec.encodeRequest(name, hashes);
        return invokeOnPartitionAsync(request, AGGREGATE_ALL_AND_ESTIMATE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateStringAsync(String value) {
        isNotNull(value, "Value");

        byte[] bytes = value.getBytes();
        long hash = HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);

        return aggregateAsync(hash);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateAllStringsAsync(String[] values) {
        isNotNull(values, "Values");

        long[] hashes = new long[values.length];

        int i = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes();
            hashes[i++] = HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
        }

        return aggregateAllAsync(hashes);
    }

    @Override
    public InternalCompletableFuture<Long> estimateAsync() {
        ClientMessage request = CardinalityEstimatorEstimateCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, ESTIMATE_DECODER);
    }
}
