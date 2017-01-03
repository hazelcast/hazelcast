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
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
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
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * {@link QuorumImpl} can be used to notify quorum service for a particular quorum result that originated externally.
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
    private QuorumFunction quorumFunction;

    // we are updating the quorum state within the single thread of membership event executor
    private volatile QuorumState quorumState = QuorumState.INITIAL;

    public QuorumImpl(QuorumConfig config, NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.config = config;
        this.quorumName = config.getName();
        this.size = config.getSize();
        initializeQuorumFunction();
    }

    public void update(Collection<Member> members) {
        QuorumState previousQuorumState = this.quorumState;
        QuorumState newQuorumState = QuorumState.ABSENT;
        try {
            boolean present = quorumFunction.apply(members);
            newQuorumState = present ? QuorumState.PRESENT : QuorumState.ABSENT;
        } catch (Exception e) {
            ILogger logger = nodeEngine.getLogger(QuorumService.class);
            logger.severe("Quorum function of quorum: " + quorumName + " failed! Quorum status is set to "
                    + newQuorumState, e);
        }

        this.quorumState = newQuorumState;
        if (previousQuorumState != newQuorumState) {
            createAndPublishEvent(members, newQuorumState == QuorumState.PRESENT);
        }
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

    public void ensureQuorumPresent(Operation op) {
        if (!isQuorumNeeded(op)) {
            return;
        }

        if (!isPresent()) {
            throw newQuorumException();
        }
    }

    private QuorumException newQuorumException() {
        if (size == 0) {
            throw new QuorumException("Cluster quorum failed");
        }
        Collection<Member> memberList = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        throw new QuorumException("Cluster quorum failed, quorum minimum size: "
                + size + ", current size: " + memberList.size());
    }

    private void createAndPublishEvent(Collection<Member> memberList, boolean presence) {
        QuorumEvent quorumEvent = new QuorumEvent(nodeEngine.getThisAddress(), size, memberList, presence);
        eventService.publishEvent(QuorumServiceImpl.SERVICE_NAME, quorumName, quorumEvent, quorumEvent.hashCode());
    }

    private void initializeQuorumFunction() {
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
                + ", isPresent=" + isPresent()
                + ", size=" + size
                + ", config=" + config
                + ", quorumFunction=" + quorumFunction
                + '}';
    }


}
