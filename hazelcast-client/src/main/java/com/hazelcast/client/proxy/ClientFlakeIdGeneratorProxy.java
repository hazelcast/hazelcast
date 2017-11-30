/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.FlakeIdGeneratorConfig;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.flakeidgen.AutoBatcher;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.ResponseParameters;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.FlakeIdGenerator;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IdBatch;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.spi.InternalCompletableFuture;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientFlakeIdGeneratorProxy extends ClientProxy implements FlakeIdGenerator {

    private static final ClientMessageDecoder NEW_ID_BATCH_DECODER = new ClientMessageDecoder() {
        @Override
        public IdBatch decodeClientMessage(ClientMessage clientMessage) {
            ResponseParameters response = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(clientMessage);
            return new IdBatch(response.base, response.increment, response.batchSize);
        }
    };

    private AutoBatcher batcher;

    public ClientFlakeIdGeneratorProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);

        FlakeIdGeneratorConfig config = getContext().getClientConfig().findFlakeIdGeneratorConfig(getName());
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidity(), new IFunction<Integer, IdBatch>() {
            @Override
            public IdBatch apply(Integer batchSize) {
                return newIdBatch(batchSize);
            }
        });
    }

    @Override
    public long newId() {
        return batcher.newId();
    }

    @Override
    public IdBatch newIdBatch(int batchSize) {
        return newIdBatchAsync(batchSize).join();
    }

    private InternalCompletableFuture<IdBatch> newIdBatchAsync(int batchSize) {
        ClientMessage request = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName()).invoke();
        return new ClientDelegatingFuture<IdBatch>(future, getSerializationService(), NEW_ID_BATCH_DECODER);
    }

    @Override
    public String toString() {
        return "FlakeIdGenerator{name='" + name + "'}";
    }
}
