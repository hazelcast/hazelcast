/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorAddCodec;
import com.hazelcast.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link CardinalityEstimator}.
 */
public class ClientCardinalityEstimatorProxy
        extends PartitionSpecificClientProxy implements CardinalityEstimator {

    public ClientCardinalityEstimatorProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public String toString() {
        return "CardinalityEstimator{" + "name='" + name + '\'' + '}';
    }

    @Override
    public void add(@Nonnull Object obj) {
        addAsync(obj).joinInternal();
    }

    @Override
    public long estimate() {
        return estimateAsync().joinInternal();
    }

    @Override
    public InternalCompletableFuture<Void> addAsync(@Nonnull Object obj) {
        checkNotNull(obj, "Object must not be null");

        Data data = toData(obj);
        ClientMessage request = CardinalityEstimatorAddCodec.encodeRequest(name, data.hash64());
        return invokeOnPartitionAsync(request, clientMessage -> null);
    }

    @Override
    public InternalCompletableFuture<Long> estimateAsync() {
        ClientMessage request = CardinalityEstimatorEstimateCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, CardinalityEstimatorEstimateCodec::decodeResponse);
    }
}
