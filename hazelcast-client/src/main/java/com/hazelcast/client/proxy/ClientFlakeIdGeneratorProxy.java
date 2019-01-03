/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.ResponseParameters;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.flakeidgen.impl.AutoBatcher;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy;
import com.hazelcast.flakeidgen.impl.IdBatch;

import static java.util.concurrent.TimeUnit.HOURS;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientFlakeIdGeneratorProxy extends ClientProxy implements FlakeIdGenerator {

    private final AutoBatcher batcher;

    public ClientFlakeIdGeneratorProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);

        ClientFlakeIdGeneratorConfig config = getContext().getClientConfig().findFlakeIdGeneratorConfig(getName());
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                new AutoBatcher.IdBatchSupplier() {
                    @Override
                    public IdBatch newIdBatch(int batchSize) {
                        return ClientFlakeIdGeneratorProxy.this.newIdBatch(batchSize);
                    }
                });
    }

    @Override
    public long newId() {
        return batcher.newId();
    }

    private IdBatch newIdBatch(int batchSize) {
        ClientMessage requestMsg = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        ClientMessage responseMsg = new ClientInvocation(getClient(), requestMsg, getName())
                .invoke().join();
        ResponseParameters response = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(responseMsg);
        return new IdBatch(response.base, response.increment, response.batchSize);
    }

    @Override
    public boolean init(long id) {
        // Add 1 hour worth of IDs as a reserve: due to long batch validity some clients might be still getting
        // older IDs. 1 hour is just a safe enough value, not a real guarantee: some clients might have longer
        // validity.
        // The init method should normally be called before any client generated IDs: in this case no reserve is
        // needed, so we don't want to increase the reserve excessively.
        long reserve = HOURS.toMillis(1)
                << (FlakeIdGeneratorProxy.BITS_NODE_ID + FlakeIdGeneratorProxy.BITS_SEQUENCE);
        return newId() >= id + reserve;
    }

    @Override
    public String toString() {
        return "FlakeIdGenerator{name='" + name + "'}";
    }
}
