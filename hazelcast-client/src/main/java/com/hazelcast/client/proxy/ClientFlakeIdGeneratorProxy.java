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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.ResponseParameters;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.concurrent.flakeidgen.FlakeIdNodeIdOverflowException;
import com.hazelcast.core.FlakeIdGenerator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Member;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.Collections.newSetFromMap;

/**
 * Proxy implementation of {@link IdGenerator}.
 */
public class ClientFlakeIdGeneratorProxy extends ClientProxy implements FlakeIdGenerator {

    private static int NUM_ATTEMPTS = 5;

    private static final ClientMessageDecoder NEW_ID_BATCH_DECODER = new ClientMessageDecoder() {
        @Override
        public IdBatch decodeClientMessage(ClientMessage clientMessage) {
            ResponseParameters response = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(clientMessage);
            return new IdBatch(response.base, response.increment, response.batchSize);
        }
    };

    /**
     * Set of member UUIDs of which we know have overflowed NodeIds. These members are never again tried
     * to get ID from.
      */
    private final Set<String> overflowedMembers = newSetFromMap(new ConcurrentHashMap<String, Boolean>(0));

    /**
     * Current randomly chosen member from which we are getting IDs.
     */
    private volatile Member randomMember;

    private AutoBatcher batcher;

    public ClientFlakeIdGeneratorProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

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
        Throwable error = null;
        for (int iteration = 0; iteration < NUM_ATTEMPTS; iteration++) {
            Member member = getRandomMember();
            try {
                return newIdBatchAsync(batchSize, member.getAddress()).join();
            } catch (Throwable e) {
                if (e instanceof FlakeIdNodeIdOverflowException) {
                    overflowedMembers.add(member.getUuid());
                    iteration--; // don't count this attempt
                }
                randomMember = null;
                error = e;
            }
        }
        throw ExceptionUtil.rethrow(error);
    }

    private InternalCompletableFuture<IdBatch> newIdBatchAsync(int batchSize, Address address) {
        ClientMessage request = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(name, batchSize);
        return invokeOnMemberAsync(request, address, NEW_ID_BATCH_DECODER);
    }

    private <T> InternalCompletableFuture<T> invokeOnMemberAsync(ClientMessage msg, Address address, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), msg, getName(), address).invoke();
        return new ClientDelegatingFuture<T>(future, getSerializationService(), decoder);
    }

    private Member getRandomMember() {
        Member member = randomMember;
        if (member == null) {
            Set<Member> members = getClient().getCluster().getMembers();
            // create list of members skipping those which we know have overflowed Node IDs
            List<Member> filteredMembers = new ArrayList<Member>(members.size());
            for (Member m : members) {
                if (!overflowedMembers.contains(m.getUuid())) {
                    filteredMembers.add(m);
                }
            }
            if (filteredMembers.isEmpty()) {
                throw new HazelcastException("All members have overflowed nodeIDs. Cluster restart is required");
            }
            member = filteredMembers.get(ThreadLocalRandomProvider.get().nextInt(filteredMembers.size()));
            randomMember = member;
        }
        return member;
    }

    @Override
    public String toString() {
        return "FlakeIdGenerator{name='" + name + "'}";
    }

    /**
     * A utility to serve IDs from IdBatch one by one, watching for validity.
     * It's a separate class due to testability.
     */
    public static class AutoBatcher {
        private final int batchSize;
        private final long validity;

        private volatile Block block = new Block(new IdBatch(0, 0, 0), 0);

        private final IFunction<Integer, IdBatch> batchIdSupplier;

        public AutoBatcher(int batchSize, long validity, IFunction<Integer, IdBatch> idGenerator) {
            this.batchSize = batchSize;
            this.validity = validity;
            this.batchIdSupplier = idGenerator;
        }

        public long newId() {
            for (;;) {
                Block block = this.block;
                long res = block.next();
                if (res != Long.MIN_VALUE) {
                    return res;
                }

                synchronized (this) {
                    if (block != this.block) {
                        // new block was assigned in the meantime
                        continue;
                    }
                    this.block = new Block(batchIdSupplier.apply(batchSize), validity);
                }
            }
        }
    }

    private static final class Block {
        private static final AtomicIntegerFieldUpdater<Block> NUM_RETURNED = AtomicIntegerFieldUpdater
                .newUpdater(Block.class, "numReturned");

        private final IdBatch idBatch;
        private final long invalidSince;
        private volatile int numReturned;

        private Block(IdBatch idBatch, long validity) {
            this.idBatch = idBatch;
            this.invalidSince = validity > 0 ? Clock.currentTimeMillis() + validity : Long.MAX_VALUE;
        }

        /**
         * Returns next ID or Long.MIN_VALUE, if there is none.
         */
        long next() {
            if (invalidSince <= Clock.currentTimeMillis()) {
                return Long.MIN_VALUE;
            }
            int index;
            do {
                index = numReturned;
                if (index == idBatch.batchSize()) {
                    return Long.MIN_VALUE;
                }
            } while (!NUM_RETURNED.compareAndSet(this, index, index + 1));
            return idBatch.base() + index * idBatch.increment();
        }
    }

    /**
     * Only for test, value is not properly synchronized.
     */
    public static void setNumAttempts(int newNumRetries) {
        NUM_ATTEMPTS = newNumRetries;
    }
}
