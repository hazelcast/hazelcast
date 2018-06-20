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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
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

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Thread.currentThread;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlakeIdGeneratorProxy
        extends AbstractDistributedObject<FlakeIdGeneratorService>
        implements FlakeIdGenerator {

    public static final int BITS_TIMESTAMP = 41;
    public static final int BITS_SEQUENCE = 6;
    public static final int BITS_NODE_ID = 16;

    /** The difference between two IDs from a sequence on single member */
    public static final long INCREMENT = 1 << BITS_NODE_ID;
    /** How far to the future is it allowed to go to generate IDs. */
    static final long ALLOWED_FUTURE_MILLIS = 15000;

    /**
     * {@code 1514764800000} is the value {@code System.currentTimeMillis()} would return on
     * 1.1.2018 0:00 UTC.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    static final long EPOCH_START = 1514764800000L;
    static final long NODE_ID_UPDATE_INTERVAL_NS = SECONDS.toNanos(2);

    private static final int NODE_ID_NOT_YET_SET = -1;
    private static final int NODE_ID_OUT_OF_RANGE = -2;

    private final String name;
    private final long epochStart;
    private final long nodeIdOffset;
    private volatile int nodeId = NODE_ID_NOT_YET_SET;
    private volatile long nextNodeIdUpdate = Long.MIN_VALUE;
    private final ILogger logger;

    /**
     * The next timestamp|seq value to be returned. The value is not shifted to most significant bits.
     */
    private final AtomicLong generatedValue = new AtomicLong(Long.MIN_VALUE);

    private volatile Member randomMember;
    private AutoBatcher batcher;

    /**
     * Set of member UUIDs of which we know have node IDs out of range. These members are never again used
     * to generate unique IDs, because this error is unrecoverable.
     */
    private final Set<String> outOfRangeMembers = newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    FlakeIdGeneratorProxy(String name, NodeEngine nodeEngine, FlakeIdGeneratorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(getClass());

        FlakeIdGeneratorConfig config = nodeEngine.getConfig().findFlakeIdGeneratorConfig(getName());
        epochStart = EPOCH_START - (config.getIdOffset() >> (BITS_SEQUENCE + BITS_NODE_ID));
        nodeIdOffset = config.getNodeIdOffset();
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                new AutoBatcher.IdBatchSupplier() {
                    @Override
                    public IdBatch newIdBatch(int batchSize) {
                        IdBatchAndWaitTime result = FlakeIdGeneratorProxy.this.newIdBatch(batchSize);
                        if (result.waitTimeMillis > 0) {
                            try {
                                Thread.sleep(result.waitTimeMillis);
                            } catch (InterruptedException e) {
                                currentThread().interrupt();
                                throw rethrow(e);
                            }
                        }
                        return result.idBatch;
                    }
                });

        if (logger.isFinestEnabled()) {
            logger.finest("Created FlakeIdGeneratorProxy, name='" + name + "'");
        }
    }

    @Override
    public long newId() {
        // The cluster version is checked when ClusterService.getMemberListJoinVersion() is called. This always happens
        // before first ID is generated.
        return batcher.newId();
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

    public IdBatchAndWaitTime newIdBatch(int batchSize) {
        int nodeId = getNodeId();

        // if we have valid node ID, generate ID locally
        if (nodeId >= 0) {
            return newIdBaseLocal(Clock.currentTimeMillis(), nodeId, batchSize);
        }

        // Remote call otherwise. Loop will end when getRandomMember() throws that all members overflowed.
        while (true) {
            NewIdBatchOperation op = new NewIdBatchOperation(name, batchSize);
            Member target = getRandomMember();
            InternalCompletableFuture<Long> future = getNodeEngine().getOperationService()
                                                                    .invokeOnTarget(getServiceName(), op, target.getAddress());
            try {
                long base = future.join();
                return new IdBatchAndWaitTime(new IdBatch(base, INCREMENT, batchSize), 0);
            } catch (NodeIdOutOfRangeException e) {
                outOfRangeMembers.add(target.getUuid());
                randomMember = null;
            }
        }
    }

    IdBatchAndWaitTime newIdBaseLocal(int batchSize) {
        return newIdBaseLocal(Clock.currentTimeMillis(), getNodeId(), batchSize);
    }

    /**
     * The layout of the ID is as follows (starting from most significant bits):<ul>
     *     <li>41 bits timestamp
     *     <li>6 bits sequence
     *     <li>16 bits node ID
     * </ul>
     *
     * This order is important: timestamp must be first to keep IDs ordered. Sequence must be second for
     * implementation reasons (it's included in {@link #generatedValue}). Node is just an appendix to make
     * IDs unique.
     *
     * @param now Current time (currentTimeMillis() normally or other value in tests)
     */
    // package-private for testing
    IdBatchAndWaitTime newIdBaseLocal(long now, int nodeId, int batchSize) {
        checkPositive(batchSize, "batchSize");
        if (nodeId == NODE_ID_OUT_OF_RANGE) {
            throw new NodeIdOutOfRangeException("NodeID overflow, this member cannot generate IDs");
        }
        assert (nodeId & -1 << BITS_NODE_ID) == 0  : "nodeId out of range: " + nodeId;
        now -= epochStart;
        if (now < -(1L << BITS_TIMESTAMP) || now >= (1L << BITS_TIMESTAMP)) {
            throw new HazelcastException("Current time out of allowed range");
        }
        now <<= BITS_SEQUENCE;
        long oldGeneratedValue;
        long base;
        do {
            oldGeneratedValue = generatedValue.get();
            base = Math.max(now, oldGeneratedValue);
        } while (!generatedValue.compareAndSet(oldGeneratedValue, base + batchSize));

        long waitTime = Math.max(0, ((base + batchSize - now) >> BITS_SEQUENCE) - ALLOWED_FUTURE_MILLIS);
        base = base << BITS_NODE_ID | nodeId;

        getService().updateStatsForBatch(name, batchSize);
        return new IdBatchAndWaitTime(new IdBatch(base, INCREMENT, batchSize), waitTime);
    }

    /**
     * Three possible return outcomes of this call:<ul>
     *     <li>returns current node ID of this member that it not out of range (a positive value)
     *     <li>returns {@link #NODE_ID_OUT_OF_RANGE}
     *     <li>throws {@link IllegalStateException}, if node ID is not yet available, with description why.
     * </ul>
     */
    private int getNodeId() {
        return getNodeId(System.nanoTime());
    }

    // package-visible for tests
    int getNodeId(long nanoTime) {
        // Check if it is a time to check for updated nodeId. We need to recheck, because if duplicate node ID
        // is assigned during a network split, this will be resolved after a cluster merge.
        // We throttle the calls to avoid contention due to the lock+unlock call in getMemberListJoinVersion().
        int nodeId = this.nodeId;
        if (nodeId != NODE_ID_OUT_OF_RANGE && nextNodeIdUpdate <= nanoTime) {
            int newNodeId = getNodeEngine().getClusterService().getMemberListJoinVersion();
            assert newNodeId >= 0 : "newNodeId=" + newNodeId;
            newNodeId += nodeIdOffset;
            nextNodeIdUpdate = nanoTime + NODE_ID_UPDATE_INTERVAL_NS;
            if (newNodeId != nodeId) {
                nodeId = newNodeId;

                // If our node ID is out of range, assign NODE_ID_OUT_OF_RANGE to nodeId
                if ((nodeId & -1 << BITS_NODE_ID) != 0) {
                    outOfRangeMembers.add(getNodeEngine().getClusterService().getLocalMember().getUuid());
                    logger.severe("Node ID is out of range (" + nodeId + "), this member won't be able to generate IDs. "
                            + "Cluster restart is recommended.");
                    nodeId = NODE_ID_OUT_OF_RANGE;
                }

                // we ignore possible double initialization
                this.nodeId = nodeId;
                if (logger.isFineEnabled()) {
                    logger.fine("Node ID assigned to '" + name + "': " + nodeId);
                }
            }
        }

        return nodeId;
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

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public static class IdBatchAndWaitTime {
        public final IdBatch idBatch;
        public final long waitTimeMillis;

        IdBatchAndWaitTime(IdBatch idBatch, long waitTimeMillis) {
            this.idBatch = idBatch;
            this.waitTimeMillis = waitTimeMillis;
        }
    }
}
