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
import com.hazelcast.core.Member;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.util.Iterator;
import java.util.Set;

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

    /**
     * Current randomly chosen member from which we are getting IDs.
     */
    private volatile Member randomMember;

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
        // go to a member for a batch of IDs
        for (;;) {
            Member member = getRandomMember();
            try {
                return newIdBatchAsync(batchSize, member.getAddress()).join();
            } catch (TargetNotMemberException e) {
                // if target member left, we'll retry with another member
                randomMember = null;
            }
        }
    }

    private InternalCompletableFuture<IdBatch> newIdBatchAsync(int batchSize, Address address) {
        ClientMessage request = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        return invokeOnMemberAsync(request, address, NEW_ID_BATCH_DECODER);
    }

    private <T> InternalCompletableFuture<T> invokeOnMemberAsync(ClientMessage msg, Address address,
                                                                 ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), msg, getName(), address).invoke();
        return new ClientDelegatingFuture<T>(future, getSerializationService(), decoder);
    }

    private Member getRandomMember() {
        Member member = randomMember;
        if (member == null) {
            Set<Member> members = getClient().getCluster().getMembers();
            int randomIndex = ThreadLocalRandomProvider.get().nextInt(members.size());
            for (Iterator<Member> iterator = members.iterator(); randomIndex >= 0; randomIndex--) {
                member = iterator.next();
            }
            randomMember = member;
        }
        return member;
    }

    @Override
    public String toString() {
        return "FlakeIdGenerator{name='" + name + "'}";
    }
}
