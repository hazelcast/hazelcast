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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReliableIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.ReliableIdGeneratorNewIdBatchCodec.ResponseParameters;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.reliableidgen.impl.AutoBatcher;
import com.hazelcast.reliableidgen.impl.IdBatch;
import com.hazelcast.config.ReliableIdGeneratorConfig;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.reliableidgen.ReliableIdGenerator;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientReliableIdGeneratorProxy extends ClientProxy implements ReliableIdGenerator {

    private final AutoBatcher batcher;

    public ClientReliableIdGeneratorProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);

        ReliableIdGeneratorConfig config = getContext().getClientConfig().findReliableIdGeneratorConfig(getName());
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                new AutoBatcher.IdBatchSupplier() {
                    @Override
                    public IdBatch newIdBatch(int batchSize) {
                        return ClientReliableIdGeneratorProxy.this.newIdBatch(batchSize);
                    }
                });
    }

    @Override
    public long newId() {
        return batcher.newId();
    }

    private IdBatch newIdBatch(int batchSize) {
        ClientMessage requestMsg = ReliableIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        ClientMessage responseMsg = new ClientInvocation(getClient(), requestMsg, getName())
                .invoke().join();
        ResponseParameters response = ReliableIdGeneratorNewIdBatchCodec.decodeResponse(responseMsg);
        return new IdBatch(response.base, response.increment, response.batchSize);
    }

    @Override
    public String toString() {
        return "ReliableIdGenerator{name='" + name + "'}";
    }
}
