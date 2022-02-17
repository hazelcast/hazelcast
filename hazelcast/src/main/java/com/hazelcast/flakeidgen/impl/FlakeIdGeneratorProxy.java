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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.flakeidgen.impl.AutoBatcher.IdBatchSupplier;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.Thread.currentThread;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlakeIdGeneratorProxy
        extends AbstractDistributedObject<FlakeIdGeneratorService>
        implements FlakeIdGenerator {

    static final long NODE_ID_UPDATE_INTERVAL_NS = SECONDS.toNanos(2);

    private static final int NODE_ID_NOT_YET_SET = -1;
    private static final int NODE_ID_OUT_OF_RANGE = -2;
    private static final int MAX_BIT_LENGTH = 63;

    private final String name;
    private final UUID source;
    private final long epochStart;
    private final long nodeIdOffset;
    private final int bitsTimestamp;
    private final int bitsSequence;
    private final int bitsNodeId;
    private final long allowedFutureMillis;
    private volatile int nodeId = NODE_ID_NOT_YET_SET;
    private volatile long nextNodeIdUpdate = Long.MIN_VALUE;
    private final long increment;
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
    private final Set<UUID> outOfRangeMembers = newSetFromMap(new ConcurrentHashMap<>());

    FlakeIdGeneratorProxy(String name, NodeEngine nodeEngine, FlakeIdGeneratorService service, UUID source) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(getClass());
        this.source = source;

        FlakeIdGeneratorConfig config = nodeEngine.getConfig().findFlakeIdGeneratorConfig(getName());
        bitsSequence = config.getBitsSequence();
        bitsNodeId = config.getBitsNodeId();
        bitsTimestamp = MAX_BIT_LENGTH - (bitsSequence + bitsNodeId);
        checkTrue(bitsTimestamp >= 0, "Configuration error, no bits left for the timestamp");
        allowedFutureMillis = config.getAllowedFutureMillis();
        increment = 1 << bitsNodeId;
        epochStart = config.getEpochStart();
        nodeIdOffset = config.getNodeIdOffset();
        batcher = new AutoBatcher(config.getPrefetchCount(), config.getPrefetchValidityMillis(),
                new IdBatchSupplier() {
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

    public IdBatchAndWaitTime newIdBatch(int batchSize) {
        int nodeId = getNodeId();

        // if we have valid node ID, generate ID locally
        if (nodeId >= 0) {
            return newIdBaseLocal(Clock.currentTimeMillis(), nodeId, batchSize);
        }

        // Remote call otherwise. Loop will end when getRandomMember() throws that all members overflowed.
        while (true) {
            NewIdBatchOperation op = new NewIdBatchOperation(name, batchSize);
            op.setCallerUuid(source);
            Member target = getRandomMember();
            InvocationFuture<Long> future = getNodeEngine().getOperationService()
                                                           .invokeOnTarget(getServiceName(), op, target.getAddress());
            try {
                long base = future.joinInternal();
                return new IdBatchAndWaitTime(new IdBatch(base, increment, batchSize), 0);
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
     *     <li>timestamp bits (41 by default)
     *     <li>sequence bits (6 by default)
     *     <li>node ID bits (16 by default)
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
        assert (nodeId & -1 << bitsNodeId) == 0  : "nodeId out of range: " + nodeId;
        now -= epochStart;
        if (now < -(1L << bitsTimestamp) || now >= (1L << bitsTimestamp)) {
            throw new HazelcastException("Current time out of allowed range");
        }
        now <<= bitsSequence;
        long oldGeneratedValue;
        long base;
        do {
            oldGeneratedValue = generatedValue.get();
            base = Math.max(now, oldGeneratedValue);
        } while (!generatedValue.compareAndSet(oldGeneratedValue, base + batchSize));

        long waitTime = Math.max(0, ((base + batchSize - now) >> bitsSequence) - allowedFutureMillis);
        base = base << bitsNodeId | nodeId;

        getService().updateStatsForBatch(name, batchSize);
        return new IdBatchAndWaitTime(new IdBatch(base, increment, batchSize), waitTime);
    }

    /**
     * Three possible return outcomes of this call:<ul>
     *     <li>returns current node ID of this member that it not out of range (a positive value)
     *     <li>returns {@link #NODE_ID_OUT_OF_RANGE}
     *     <li>throws {@link IllegalStateException}, if node ID is not yet available, with description why.
     * </ul>
     */
    private int getNodeId() {
        int nodeId = getNodeId(System.nanoTime());
        assert nodeId > 0 || nodeId == NODE_ID_OUT_OF_RANGE : "getNodeId() returned invalid value: " + nodeId;
        return nodeId;
    }

    // package-visible for tests
    int getNodeId(long nanoTime) {
        // Check if it is a time to check for updated nodeId. We need to recheck, because if duplicate node ID
        // is assigned during a network split, this will be resolved after a cluster merge.
        // We throttle the calls to avoid contention due to the lock+unlock call in getMemberListJoinVersion().
        long localNextNodeIdUpdate = this.nextNodeIdUpdate;
        int localNodeId = this.nodeId;
        if (localNextNodeIdUpdate <= nanoTime) {
            if (localNodeId == NODE_ID_OUT_OF_RANGE) {
                return localNodeId;
            }
            int newNodeId = getNodeEngine().getClusterService().getMemberListJoinVersion();
            assert newNodeId >= 0 : "newNodeId=" + newNodeId;
            newNodeId += nodeIdOffset;
            if (newNodeId != localNodeId) {
                localNodeId = newNodeId;

                // If our node ID is out of range, assign NODE_ID_OUT_OF_RANGE to nodeId
                if ((localNodeId & -1 << bitsNodeId) != 0) {
                    outOfRangeMembers.add(getNodeEngine().getClusterService().getLocalMember().getUuid());
                    logger.severe("Node ID is out of range (" + localNodeId
                            + "), this member won't be able to generate IDs. Cluster restart is recommended.");
                    localNodeId = NODE_ID_OUT_OF_RANGE;
                }

                // we ignore possible double initialization
                this.nodeId = localNodeId;
                this.nextNodeIdUpdate = nanoTime + NODE_ID_UPDATE_INTERVAL_NS;
                if (logger.isFineEnabled()) {
                    logger.fine("Node ID assigned to '" + name + "': " + localNodeId);
                }
            }
        }
       return localNodeId;
    }

    private Member getRandomMember() {
        Member member = randomMember;
        if (member == null) {
            // if local member is in outOfRangeMembers, use random member
            Set<Member> members = getNodeEngine().getClusterService().getMembers();
            List<Member> filteredMembers = new ArrayList<>(members.size());
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
