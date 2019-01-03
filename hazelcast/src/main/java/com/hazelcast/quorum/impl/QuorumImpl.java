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

package com.hazelcast.quorum.impl;

import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.quorum.HeartbeatAware;
import com.hazelcast.quorum.PingAware;
import com.hazelcast.quorum.Quorum;
import com.hazelcast.quorum.QuorumEvent;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.QuorumCheckAwareOperation;
import com.hazelcast.spi.impl.eventservice.InternalEventService;

import java.util.Collection;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * {@link QuorumImpl} can be used to notify quorum service for a particular quorum result that originated externally.
 * <p>
 * IMPORTANT: The term "quorum" simply refers to the count of members in the cluster required for an operation to succeed.
 * It does NOT refer to an implementation of Paxos or Raft protocols as used in many NoSQL and distributed systems.
 * The mechanism it provides in Hazelcast protects the user in case the number of nodes in a cluster drops below the
 * specified one.
 */
public class QuorumImpl implements Quorum {

    private enum QuorumState {
        INITIAL,
        PRESENT,
        ABSENT
    }

    private final NodeEngineImpl nodeEngine;
    private final String quorumName;
    private final int size;
    private final QuorumConfig config;
    private final InternalEventService eventService;
    private final QuorumFunction quorumFunction;
    private final boolean heartbeatAwareQuorumFunction;
    private final boolean pingAwareQuorumFunction;
    private final boolean membershipListenerQuorumFunction;

    /**
     * Current quorum state. Updated by single thread, read by multiple threads.
     */
    private volatile QuorumState quorumState = QuorumState.INITIAL;

    QuorumImpl(QuorumConfig config, NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.config = config;
        this.quorumName = config.getName();
        this.size = config.getSize();
        this.quorumFunction = initializeQuorumFunction();
        this.heartbeatAwareQuorumFunction = (quorumFunction instanceof HeartbeatAware);
        this.membershipListenerQuorumFunction = (quorumFunction instanceof MembershipListener);
        this.pingAwareQuorumFunction = (quorumFunction instanceof PingAware);
    }

    /**
     * Determines if the quorum is present for the given member collection, caches the result and publishes an event under
     * the {@link #quorumName} topic if there was a change in presence.
     * <p/>
     * <strong>This method is not thread safe and should not be called concurrently.</strong>
     *
     * @param members the members for which the presence is determined
     */
    void update(Collection<Member> members) {
        QuorumState previousQuorumState = quorumState;
        QuorumState newQuorumState = QuorumState.ABSENT;
        try {
            boolean present = quorumFunction.apply(members);
            newQuorumState = present ? QuorumState.PRESENT : QuorumState.ABSENT;
        } catch (Exception e) {
            ILogger logger = nodeEngine.getLogger(QuorumService.class);
            logger.severe("Quorum function of quorum: " + quorumName + " failed! Quorum status is set to "
                    + newQuorumState, e);
        }

        quorumState = newQuorumState;
        if (previousQuorumState != newQuorumState) {
            createAndPublishEvent(members, newQuorumState == QuorumState.PRESENT);
        }
    }

    /**
     * Notify a {@link HeartbeatAware} {@code QuorumFunction} that a heartbeat has been received from a member.
     *
     * @param member    source member
     * @param timestamp heartbeat's timestamp
     */
    void onHeartbeat(Member member, long timestamp) {
        if (!heartbeatAwareQuorumFunction) {
            return;
        }
        ((HeartbeatAware) quorumFunction).onHeartbeat(member, timestamp);
    }

    void onPing(Member member, boolean successful) {
        if (!pingAwareQuorumFunction) {
            return;
        }
        PingAware pingAware = (PingAware) quorumFunction;
        if (successful) {
            pingAware.onPingRestored(member);
        } else {
            pingAware.onPingLost(member);
        }

    }

    void onMemberAdded(MembershipEvent event) {
        if (!membershipListenerQuorumFunction) {
            return;
        }
        ((MembershipListener) quorumFunction).memberAdded(event);
    }

    void onMemberRemoved(MembershipEvent event) {
        if (!membershipListenerQuorumFunction) {
            return;
        }
        ((MembershipListener) quorumFunction).memberRemoved(event);
    }

    public String getName() {
        return quorumName;
    }

    public int getSize() {
        return size;
    }

    public QuorumConfig getConfig() {
        return config;
    }

    @Override
    public boolean isPresent() {
        return quorumState == QuorumState.PRESENT;
    }

    /**
     * Indicates whether the {@link #quorumFunction} is {@link HeartbeatAware}. If so, then member heartbeats will be published
     * to the {@link #quorumFunction}.
     *
     * @return {@code true} when the {@link #quorumFunction} implements {@link HeartbeatAware}, otherwise {@code false}
     */
    boolean isHeartbeatAware() {
        return heartbeatAwareQuorumFunction;
    }

    /**
     * Indicates whether the {@link #quorumFunction} is {@link PingAware}. If so, then ICMP pings will be published
     * to the {@link #quorumFunction}.
     *
     * @return {@code true} when the {@link #quorumFunction} implements {@link PingAware}, otherwise {@code false}
     */
    boolean isPingAware() {
        return pingAwareQuorumFunction;
    }

    /**
     * Returns if quorum is needed for this operation.
     * The quorum is determined by the {@link QuorumConfig#type} and by the type of the operation -
     * {@link ReadonlyOperation} or {@link MutatingOperation}.
     *
     * @param op the operation which is to be executed
     * @return if this quorum should be consulted for this operation
     * @throws IllegalArgumentException if the quorum configuration type is not handled
     */
    private boolean isQuorumNeeded(Operation op) {
        QuorumType type = config.getType();
        switch (type) {
            case WRITE:
                return isWriteOperation(op) && shouldCheckQuorum(op);
            case READ:
                return isReadOperation(op) && shouldCheckQuorum(op);
            case READ_WRITE:
                return (isReadOperation(op) || isWriteOperation(op)) && shouldCheckQuorum(op);
            default:
                throw new IllegalStateException("Unhandled quorum type: " + type);
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
     * Returns {@code true} if the operation allows checking for quorum,
     * {@code false} if the quorum check does not apply to this operation.
     */
    private static boolean shouldCheckQuorum(Operation op) {
        return !(op instanceof QuorumCheckAwareOperation) || ((QuorumCheckAwareOperation) op).shouldCheckQuorum();
    }

    /**
     * Ensures that the quorum is present for the given operation. First checks if the quorum type defined by the configuration
     * covers this operation and checks if the quorum is present. Dispatches an event under the {@link #quorumName} topic
     * if membership changed after determining the quorum presence.
     *
     * @param op the operation for which the quorum should be present
     * @throws QuorumException if the operation requires a quorum and the quorum is not present
     */
    void ensureQuorumPresent(Operation op) {
        if (!isQuorumNeeded(op)) {
            return;
        }
        ensureQuorumPresent();
    }

    void ensureQuorumPresent() {
        if (!isPresent()) {
            throw newQuorumException();
        }
    }

    private QuorumException newQuorumException() {
        throw new QuorumException("Split brain protection exception: " + quorumName + " has failed!");
    }

    private void createAndPublishEvent(Collection<Member> memberList, boolean presence) {
        QuorumEvent quorumEvent = new QuorumEvent(nodeEngine.getThisAddress(), size, memberList, presence);
        eventService.publishEvent(QuorumServiceImpl.SERVICE_NAME, quorumName, quorumEvent, quorumEvent.hashCode());
    }

    private QuorumFunction initializeQuorumFunction() {
        QuorumFunction quorumFunction = config.getQuorumFunctionImplementation();
        if (quorumFunction == null && config.getQuorumFunctionClassName() != null) {
            try {
                quorumFunction = newInstance(nodeEngine.getConfigClassLoader(), config.getQuorumFunctionClassName());
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
        if (quorumFunction == null) {
            quorumFunction = new MemberCountQuorumFunction(size);
        }
        ManagedContext managedContext = nodeEngine.getSerializationService().getManagedContext();
        quorumFunction = (QuorumFunction) managedContext.initialize(quorumFunction);
        return quorumFunction;
    }

    @Override
    public String toString() {
        return "QuorumImpl{"
                + "quorumName='" + quorumName + '\''
                + ", isPresent=" + isPresent()
                + ", size=" + size
                + ", config=" + config
                + ", quorumFunction=" + quorumFunction
                + '}';
    }
}
