/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.splitbrainprotection.impl;

import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.splitbrainprotection.HeartbeatAware;
import com.hazelcast.splitbrainprotection.PingAware;
import com.hazelcast.splitbrainprotection.SplitBrainProtection;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionEvent;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.SplitBrainProtectionCheckAwareOperation;

import java.util.Collection;

import static com.hazelcast.internal.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * {@link SplitBrainProtectionImpl} can be used to notify split brain protection service for a particular split brain protection
 * result that originated externally.
 */
public class SplitBrainProtectionImpl implements SplitBrainProtection {

    private enum SplitBrainProtectionState {
        INITIAL,
        HAS_MIN_CLUSTER_SIZE,
        NO_MIN_CLUSTER_SIZE
    }

    private final NodeEngineImpl nodeEngine;
    private final String splitBrainProtectionName;
    private final int minimumClusterSize;
    private final SplitBrainProtectionConfig config;
    private final EventService eventService;
    private final SplitBrainProtectionFunction splitBrainProtectionFunction;
    private final boolean heartbeatAwareSplitBrainProtectionFunction;
    private final boolean pingAwareSplitBrainProtectionFunction;
    private final boolean membershipListenerSplitBrainProtectionFunction;

    /**
     * Current split brain protection state. Updated by single thread, read by multiple threads.
     */
    private volatile SplitBrainProtectionState splitBrainProtectionState = SplitBrainProtectionState.INITIAL;

    SplitBrainProtectionImpl(SplitBrainProtectionConfig config, NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.config = config;
        this.splitBrainProtectionName = config.getName();
        this.minimumClusterSize = config.getMinimumClusterSize();
        this.splitBrainProtectionFunction = initializeSplitBrainProtectionFunction();
        this.heartbeatAwareSplitBrainProtectionFunction = (splitBrainProtectionFunction instanceof HeartbeatAware);
        this.membershipListenerSplitBrainProtectionFunction = (splitBrainProtectionFunction instanceof MembershipListener);
        this.pingAwareSplitBrainProtectionFunction = (splitBrainProtectionFunction instanceof PingAware);
    }

    /**
     * Determines if the split brain protection is present for the given member collection, caches the result and
     * publishes an event under the {@link #splitBrainProtectionName} topic if there was a change in presence.
     * <p>
     * <strong>This method is not thread safe and should not be called concurrently.</strong>
     *
     * @param members the members for which the presence is determined
     */
    void update(Collection<Member> members) {
        SplitBrainProtectionState previousSplitBrainProtectionState = splitBrainProtectionState;
        SplitBrainProtectionState newSplitBrainProtectionState = SplitBrainProtectionState.NO_MIN_CLUSTER_SIZE;
        try {
            boolean present = splitBrainProtectionFunction.apply(members);
            newSplitBrainProtectionState = present ? SplitBrainProtectionState.HAS_MIN_CLUSTER_SIZE
                    : SplitBrainProtectionState.NO_MIN_CLUSTER_SIZE;
        } catch (Exception e) {
            ILogger logger = nodeEngine.getLogger(SplitBrainProtectionService.class);
            logger.severe("Split brain protection function of split brain protection: "
                    + splitBrainProtectionName + " failed! Split brain protection status is set to "
                    + newSplitBrainProtectionState, e);
        }

        if (previousSplitBrainProtectionState == SplitBrainProtectionState.INITIAL
                && newSplitBrainProtectionState != SplitBrainProtectionState.HAS_MIN_CLUSTER_SIZE) {
            // We should not set the new quorum state until quorum is met the first time
            // after local member joins the cluster.
            return;
        }

        splitBrainProtectionState = newSplitBrainProtectionState;

        if (previousSplitBrainProtectionState == SplitBrainProtectionState.INITIAL) {
            // We should not fire any quorum events before quorum is present the first time
            // when local member joins the cluster.
            return;
        }

        if (previousSplitBrainProtectionState != newSplitBrainProtectionState) {
            createAndPublishEvent(members, newSplitBrainProtectionState == SplitBrainProtectionState.HAS_MIN_CLUSTER_SIZE);
        }
    }

    /**
     * Notify a {@link HeartbeatAware} {@code SplitBrainProtectionFunction} that a heartbeat has been received from a member.
     *
     * @param member    source member
     * @param timestamp heartbeat's timestamp
     */
    void onHeartbeat(Member member, long timestamp) {
        if (!heartbeatAwareSplitBrainProtectionFunction) {
            return;
        }
        ((HeartbeatAware) splitBrainProtectionFunction).onHeartbeat(member, timestamp);
    }

    void onPing(Member member, boolean successful) {
        if (!pingAwareSplitBrainProtectionFunction) {
            return;
        }
        PingAware pingAware = (PingAware) splitBrainProtectionFunction;
        if (successful) {
            pingAware.onPingRestored(member);
        } else {
            pingAware.onPingLost(member);
        }

    }

    void onMemberAdded(MembershipEvent event) {
        if (!membershipListenerSplitBrainProtectionFunction) {
            return;
        }
        ((MembershipListener) splitBrainProtectionFunction).memberAdded(event);
    }

    void onMemberRemoved(MembershipEvent event) {
        if (!membershipListenerSplitBrainProtectionFunction) {
            return;
        }
        ((MembershipListener) splitBrainProtectionFunction).memberRemoved(event);
    }

    public String getName() {
        return splitBrainProtectionName;
    }

    public int getMinimumClusterSize() {
        return minimumClusterSize;
    }

    public SplitBrainProtectionConfig getConfig() {
        return config;
    }

    @Override
    public boolean hasMinimumSize() {
        return splitBrainProtectionState == SplitBrainProtectionState.HAS_MIN_CLUSTER_SIZE;
    }

    /**
     * Indicates whether the {@link #splitBrainProtectionFunction} is {@link HeartbeatAware}. If so, then member
     * heartbeats will be published to the {@link #splitBrainProtectionFunction}.
     *
     * @return {@code true} when the {@link #splitBrainProtectionFunction} implements {@link HeartbeatAware},
     * otherwise {@code false}
     */
    boolean isHeartbeatAware() {
        return heartbeatAwareSplitBrainProtectionFunction;
    }

    /**
     * Indicates whether the {@link #splitBrainProtectionFunction} is {@link PingAware}. If so, then ICMP pings will be published
     * to the {@link #splitBrainProtectionFunction}.
     *
     * @return {@code true} when the {@link #splitBrainProtectionFunction} implements {@link PingAware}, otherwise {@code false}
     */
    boolean isPingAware() {
        return pingAwareSplitBrainProtectionFunction;
    }

    /**
     * Returns if split brain protection is needed for this operation.
     * The split brain protection is determined by the {@link SplitBrainProtectionConfig#getProtectOn()} and by the type of the
     * operation - {@link ReadonlyOperation} or {@link MutatingOperation}.
     *
     * @param op the operation which is to be executed
     * @return if this split brain protection should be consulted for this operation
     * @throws IllegalArgumentException if the split brain protection configuration type is not handled
     */
    private boolean isSplitBrainProtectionNeeded(Operation op) {
        SplitBrainProtectionOn type = config.getProtectOn();
        switch (type) {
            case WRITE:
                return isWriteOperation(op) && shouldCheckSplitBrainProtection(op);
            case READ:
                return isReadOperation(op) && shouldCheckSplitBrainProtection(op);
            case READ_WRITE:
                return (isReadOperation(op) || isWriteOperation(op)) && shouldCheckSplitBrainProtection(op);
            default:
                throw new IllegalStateException("Unhandled split brain protection type: " + type);
        }
    }

    /**
     * Returns {@code true} if this operation is marked as a read-only operation.
     * If this method returns {@code false}, the operation still might be
     * read-only but is not marked as such.
     */
    private static boolean isReadOperation(Operation op) {
        return op instanceof ReadonlyOperation;
    }

    /**
     * Returns {@code true} if this operation is marked as a mutating operation.
     * If this method returns {@code false}, the operation still might be
     * mutating but is not marked as such.
     */
    private static boolean isWriteOperation(Operation op) {
        return op instanceof MutatingOperation;
    }

    /**
     * Returns {@code true} if the operation allows checking for split brain protection,
     * {@code false} if the split brain protection check does not apply to this operation.
     */
    private static boolean shouldCheckSplitBrainProtection(Operation op) {
        return !(op instanceof SplitBrainProtectionCheckAwareOperation)
                || ((SplitBrainProtectionCheckAwareOperation) op).shouldCheckSplitBrainProtection();
    }

    /**
     * Ensures that the minimum cluster size property is satisfied (for the purpose of split brain detection)
     * for the given operation. First checks if the split brain protection type defined by the configuration covers
     * this operation and checks if the split brain protection is present.
     * Dispatches an event under the {@link #splitBrainProtectionName} topic
     * if membership changed after determining the minimum cluster size property is satisfied.
     *
     * @param op the operation for which the minimum cluster size property should be satisfied
     * @throws SplitBrainProtectionException if the operation requires a split brain protection and the
     * minimum cluster size property is not satisfied.
     */
    void ensureNoSplitBrain(Operation op) {
        if (!isSplitBrainProtectionNeeded(op)) {
            return;
        }
        ensureNoSplitBrain();
    }

    void ensureNoSplitBrain() {
        if (!hasMinimumSize()) {
            throw newSplitBrainProtectionException();
        }
    }

    private SplitBrainProtectionException newSplitBrainProtectionException() {
        throw new SplitBrainProtectionException("Split brain protection exception: " + splitBrainProtectionName + " has failed!");
    }

    private void createAndPublishEvent(Collection<Member> memberList, boolean presence) {
        SplitBrainProtectionEvent splitBrainProtectionEvent = new SplitBrainProtectionEvent(nodeEngine.getThisAddress(),
                minimumClusterSize, memberList, presence);
        eventService.publishEvent(SplitBrainProtectionServiceImpl.SERVICE_NAME, splitBrainProtectionName,
                splitBrainProtectionEvent, splitBrainProtectionEvent.hashCode());
    }

    private SplitBrainProtectionFunction initializeSplitBrainProtectionFunction() {
        SplitBrainProtectionFunction splitBrainProtectionFunction = config.getFunctionImplementation();
        if (splitBrainProtectionFunction == null && config.getFunctionClassName() != null) {
            try {
                splitBrainProtectionFunction = newInstance(nodeEngine.getConfigClassLoader(),
                        config.getFunctionClassName());
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
        if (splitBrainProtectionFunction == null) {
            splitBrainProtectionFunction = new MemberCountSplitBrainProtectionFunction(minimumClusterSize);
        }
        ManagedContext managedContext = nodeEngine.getSerializationService().getManagedContext();
        splitBrainProtectionFunction = (SplitBrainProtectionFunction) managedContext.initialize(splitBrainProtectionFunction);
        return splitBrainProtectionFunction;
    }

    @Override
    public String toString() {
        return "SplitBrainProtectionImpl{"
                + "splitBrainProtectionName='" + splitBrainProtectionName + '\''
                + ", isMinimumClusterSizeSatisfied=" + hasMinimumSize()
                + ", minimumClusterSize=" + minimumClusterSize
                + ", config=" + config
                + ", splitBrainProtectionFunction=" + splitBrainProtectionFunction
                + '}';
    }
}
