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

package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.FlakeIdGenerator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.Preconditions.checkPositive;
import static java.util.Collections.newSetFromMap;

public class FlakeIdGeneratorProxy
        extends AbstractDistributedObject<FlakeIdGeneratorService>
        implements FlakeIdGenerator {

    public static final int BITS_TIMESTAMP = 42;
    public static final int BITS_SEQUENCE = 6;
    public static final int BITS_NODE_ID = 16;

    public static final long INCREMENT = 1 << BITS_NODE_ID;

    /**
     * 1.1.2017 0:00 GMT will be MIN_VALUE in the 42-bit signed integer.
     * <p>
     * {@code 1483228800000} is the value {@code System.currentTimeMillis()} would return on
     * 1.1.2017 0:00 GMT.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    static final long EPOCH_START = 1483228800000L + (1L << (BITS_TIMESTAMP - 1));


    /** Assigned node ID or -1, if it is out of range */
    private final int nodeId;
    private final String name;
    private final ILogger logger;

    /**
     * The highest timestamp|seq value returned so far. The value is not shifted to most significant bits.
     */
    private final AtomicLong generatedValue = new AtomicLong(Long.MIN_VALUE);

    private volatile Member randomMember;
    private AutoBatcher batcher;

    /**
     * Set of member UUIDs of which we know have node IDs out of range. These members are never again used
     * to generate unique IDs, because this error is unrecoverable.
     */
    private final Set<String> outOfRangeMembers;

    FlakeIdGeneratorProxy(String name, int nodeId, NodeEngine nodeEngine, FlakeIdGeneratorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(getClass());

        // If our nodeId is out of range, ID generation will be forwarded to another member.
        // This is designed to provide functionality when the node ID overflows for new members but there are
        // still some members with good node ID.
        if ((nodeId & -1 << BITS_NODE_ID) != 0) {
            this.nodeId = -1;
            outOfRangeMembers = newSetFromMap(new ConcurrentHashMap<String, Boolean>());
            outOfRangeMembers.add(nodeEngine.getClusterService().getLocalMember().getUuid());
            logger.severe("Node ID is out of range (" + nodeId + "), this member won't be able to generate IDs. "
                    + "Cluster restart is recommended.");
        } else {
            this.nodeId = nodeId;
            outOfRangeMembers = null;
        }

        FlakeIdGeneratorConfig config = nodeEngine.getConfig().findFlakeIdGeneratorConfig(getName());
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                new AutoBatcher.IdBatchSupplier() {
                    @Override
                    public IdBatch newIdBatch(int batchSize) {
                        return FlakeIdGeneratorProxy.this.newIdBatch(batchSize);
                    }
                });
    }

    @Override
    public long newId() {
        return batcher.newId();
    }

    public IdBatch newIdBatch(int batchSize) {
        // local operation if we have valid node ID
        if (nodeId >= 0) {
            long base = newIdBaseLocal(batchSize);
            return new IdBatch(base, INCREMENT, batchSize);
        }

        // remote call otherwise
        while (true) {
            NewIdBatchOperation op = new NewIdBatchOperation(name, batchSize);
            Member target = getRandomMember();
            InternalCompletableFuture<Long> future = getNodeEngine().getOperationService()
                                                                    .invokeOnTarget(getServiceName(), op, target.getAddress());
            try {
                long base = future.join();
                return new IdBatch(base, 1 << BITS_NODE_ID, batchSize);
            } catch (FlakeIdNodeIdOutOfRangeException e) {
                outOfRangeMembers.add(target.getUuid());
                randomMember = null;
            }
        }
    }

    long newIdBaseLocal(int batchSize) {
        return newIdBaseLocal(Clock.currentTimeMillis(), batchSize);
    }

    /**
     * The layout of the ID is as follows (starting from most significant bits):<ul>
     *     <li>42 bits timestamp
     *     <li>6 bits sequence
     *     <li>16 bits node ID
     * </ul>
     *
     * This order is important: timestamp must be first to keep IDs ordered. Sequence must be second for
     * implementation reasons (it's included in {@link #generatedValue}). Node is just an appendix to make
     * IDs unique.
     *
     * @param now Current time (currentTimeMillis() normally or other value in tests)
     * @return First ID for the batch.
     */
    long newIdBaseLocal(long now, int batchSize) {
        checkPositive(batchSize, "batchSize");
        if (nodeId < 0) {
            throw new FlakeIdNodeIdOutOfRangeException("NodeID overflow, this member cannot generate IDs");
        }
        now -= EPOCH_START;
        assert now >= -(1L << BITS_TIMESTAMP - 1) && now < (1L << BITS_TIMESTAMP - 1)  : "Current time out of allowed range";
        now <<= BITS_SEQUENCE;
        long oldGeneratedValue;
        long base;
        do {
            oldGeneratedValue = generatedValue.get();
            base = Math.max(now, oldGeneratedValue);
        } while (!generatedValue.compareAndSet(oldGeneratedValue, base + batchSize));
        return base << BITS_NODE_ID | nodeId;
    }

    private Member getRandomMember() {
        Member member = randomMember;
        if (member == null) {
            // if local member is in outOfRangeMembers, use random member
            Set<Member> members = getNodeEngine().getClusterService().getMembers();
            List<Member> filteredMembers = new ArrayList<Member>(members.size());
            for (Member m : members) {
                if (!outOfRangeMembers.contains(m.getUuid())) {
                    filteredMembers.add(m);
                }
            }
            if (filteredMembers.isEmpty()) {
                throw new HazelcastException("All members have node ID out of range. Cluster restart is required");
            }
            member = filteredMembers.get(ThreadLocalRandomProvider.get().nextInt(filteredMembers.size()));
            randomMember = member;
        }
        return member;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return FlakeIdGeneratorService.SERVICE_NAME;
    }
}
