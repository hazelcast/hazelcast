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
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.ResponseParameters;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.concurrent.flakeidgen.AutoBatcher;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.FlakeIdGenerator;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IdBatch;
import com.hazelcast.core.IdGenerator;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientFlakeIdGeneratorProxy extends ClientProxy implements FlakeIdGenerator {

    private final AutoBatcher batcher;

    public ClientFlakeIdGeneratorProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);

        FlakeIdGeneratorConfig config = getContext().getClientConfig().findFlakeIdGeneratorConfig(getName());
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                new IFunction<Integer, IdBatch>() {
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
        ClientMessage requestMsg = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        ClientMessage responseMsg = new ClientInvocation(getClient(), requestMsg, getName())
                .invoke().join();
        ResponseParameters response = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(responseMsg);
        return new IdBatch(response.base, response.increment, response.batchSize);
    }

    @Override
    public String toString() {
        return "FlakeIdGenerator{name='" + name + "'}";
    }
}
