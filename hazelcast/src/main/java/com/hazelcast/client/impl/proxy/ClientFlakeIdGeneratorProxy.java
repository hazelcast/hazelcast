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

import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.ResponseParameters;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.flakeidgen.impl.AutoBatcher;
import com.hazelcast.flakeidgen.impl.IdBatch;

/**
 * Proxy implementation of {@link FlakeIdGenerator}.
 */
public class ClientFlakeIdGeneratorProxy extends ClientProxy implements FlakeIdGenerator {

    private final AutoBatcher batcher;

    public ClientFlakeIdGeneratorProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);

        ClientFlakeIdGeneratorConfig config = getContext().getClientConfig().findFlakeIdGeneratorConfig(getName());
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                ClientFlakeIdGeneratorProxy.this::newIdBatch);
    }

    @Override
    public long newId() {
        return batcher.newId();
    }

    private IdBatch newIdBatch(int batchSize) {
        ClientMessage requestMsg = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        ClientMessage responseMsg = new ClientInvocation(getClient(), requestMsg, getName())
                .invoke().joinInternal();
        ResponseParameters response = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(responseMsg);
        return new IdBatch(response.base, response.increment, response.batchSize);
    }

    @Override
    public String toString() {
        return "FlakeIdGenerator{name='" + name + "'}";
    }
}
