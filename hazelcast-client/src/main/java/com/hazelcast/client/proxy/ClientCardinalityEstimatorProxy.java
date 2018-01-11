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

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link CardinalityEstimator}.
 */
public class ClientCardinalityEstimatorProxy
        extends PartitionSpecificClientProxy implements CardinalityEstimator {

    private static final ClientMessageDecoder ADD_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder ESTIMATE_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CardinalityEstimatorEstimateCodec.decodeResponse(clientMessage).response;
        }
    };

    public ClientCardinalityEstimatorProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public String toString() {
        return "CardinalityEstimator{" + "name='" + name + '\'' + '}';
    }

    @Override
    public void add(Object obj) {
        addAsync(obj).join();
    }

    @Override
    public long estimate() {
        return estimateAsync().join();
    }

    @Override
    public InternalCompletableFuture<Void> addAsync(Object obj) {
        checkNotNull(obj, "Object is null");

        Data data = toData(obj);
        ClientMessage request = CardinalityEstimatorAddCodec.encodeRequest(name, data.hash64());
        return invokeOnPartitionAsync(request, ADD_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> estimateAsync() {
        ClientMessage request = CardinalityEstimatorEstimateCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, ESTIMATE_DECODER);
    }
}
