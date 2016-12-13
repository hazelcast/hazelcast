/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.Quorum;
import com.hazelcast.quorum.QuorumEvent;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * {@link QuorumImpl} can be used to notify quorum service for a particular quorum result that originated externally.
 */
public class QuorumImpl implements Quorum {

    private final AtomicBoolean isPresent = new AtomicBoolean(true);
    private final AtomicBoolean lastPresence = new AtomicBoolean(true);

    private final NodeEngineImpl nodeEngine;
    private final String quorumName;
    private final int size;
    private final QuorumConfig config;
    private final InternalEventService eventService;
    private QuorumFunction quorumFunction;

    private volatile boolean initialized;

    public QuorumImpl(QuorumConfig config, NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.config = config;
        this.quorumName = config.getName();
        this.size = config.getSize();
        initializeQuorumFunction(nodeEngine.getHazelcastInstance());
    }

    /**
     * Determines if the quorum is present for the given member collection, caches the result and publishes an event under
     * the {@link #quorumName} topic if there was a change in presence.
     *
     * @param members the members for which the presence is determined
     */
    public void update(Collection<Member> members) {
        boolean presence = quorumFunction.apply(members);
        setLocalResult(presence);
        updateLastResultAndFireEvent(members, presence);
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

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public boolean isPresent() {
        return isPresent.get();
    }

    /**
     * Sets the current quorum presence to the given {@code presence} and marks the instance as initialized.
     *
     * @param presence the quorum presence to be set
     */
    public void setLocalResult(boolean presence) {
        setLocalResultInternal(presence);
    }

    private void setLocalResultInternal(boolean presence) {
        this.initialized = true;
        this.isPresent.set(presence);
    }

    /**
     * Returns if quorum is needed for this operation. This is determined by the {@link QuorumConfig#type} and by the type
     * of the operation - {@link ReadonlyOperation} or {@link MutatingOperation}.
     *
     * @param op the operation which is to be executed
     * @return if this quorum should be consulted for this operation
     * @throws IllegalArgumentException if the quorum configuration type is not handled
     */
    private boolean isQuorumNeeded(Operation op) {
        QuorumType type = config.getType();
        switch (type) {
            case WRITE:
                return isWriteOperation(op);
            case READ:
                return isReadOperation(op);
            case READ_WRITE:
                return isReadOperation(op) || isWriteOperation(op);
            default:
                throw new IllegalStateException("Unhandled quorum type: " + type);
        }
    }

    private static boolean isReadOperation(Operation op) {
        return op instanceof ReadonlyOperation;
    }

    private static boolean isWriteOperation(Operation op) {
        return op instanceof MutatingOperation;
    }

    /**
     * Ensures that the quorum is present for the given operation. First checks if the quorum type defined by the configuration
     * covers this operation and checks if the quorum is present. Dispatches an event under the {@link #quorumName} topic
     * if membership changed after determining the quorum presence.
     *
     * @param op the operation for which the quorum should be present
     * @throws QuorumException if the operation requires a quorum and the quorum is not present
     */
    public void ensureQuorumPresent(Operation op) {
        if (!isQuorumNeeded(op)) {
            return;
        }
        Collection<Member> memberList = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        if (!isInitialized()) {
            update(memberList);
        }
        if (!isPresent()) {
            updateLastResultAndFireEvent(memberList, false);
            throw newQuorumException(memberList);
        }
        updateLastResultAndFireEvent(memberList, true);
    }

    private QuorumException newQuorumException(Collection<Member> memberList) {
        if (size == 0) {
            throw new QuorumException("Cluster quorum failed");
        }
        throw new QuorumException("Cluster quorum failed, quorum minimum size: "
                + size + ", current size: " + memberList.size());
    }


    private void updateLastResultAndFireEvent(Collection<Member> memberList, Boolean presence) {
        for (; ; ) {
            boolean currentPresence = lastPresence.get();
            if (presence.equals(currentPresence)) {
                return;
            }
            if (lastPresence.compareAndSet(currentPresence, presence)) {
                createAndPublishEvent(memberList, presence);
                return;
            }
        }
    }

    private void createAndPublishEvent(Collection<Member> memberList, boolean presence) {
        QuorumEvent quorumEvent = new QuorumEvent(nodeEngine.getThisAddress(), size, memberList, presence);
        eventService.publishEvent(QuorumServiceImpl.SERVICE_NAME, quorumName, quorumEvent, quorumEvent.hashCode());
    }

    private void initializeQuorumFunction(HazelcastInstance hazelcastInstance) {
        if (config.getQuorumFunctionImplementation() != null) {
            quorumFunction = config.getQuorumFunctionImplementation();
        } else if (config.getQuorumFunctionClassName() != null) {
            try {
                quorumFunction = ClassLoaderUtil
                        .newInstance(nodeEngine.getConfigClassLoader(), config.getQuorumFunctionClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (quorumFunction == null) {
            quorumFunction = new MemberCountQuorumFunction();
        }
        if (quorumFunction instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) quorumFunction).setHazelcastInstance(hazelcastInstance);
        }
    }

    private class MemberCountQuorumFunction implements QuorumFunction {
        @Override
        public boolean apply(Collection<Member> members) {
            return members.size() >= size;
        }
    }

    @Override
    public String toString() {
        return "QuorumImpl{"
                + "quorumName='" + quorumName + '\''
                + ", isPresent=" + isPresent
                + ", size=" + size
                + ", config=" + config
                + ", quorumFunction=" + quorumFunction
                + ", initialized=" + initialized
                + '}';
    }
}
