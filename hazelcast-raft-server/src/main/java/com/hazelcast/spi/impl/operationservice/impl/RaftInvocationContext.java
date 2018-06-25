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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.service.RaftGroupInfo;
import com.hazelcast.raft.impl.service.RaftService;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *  Contains all static dependencies for a {@link RaftInvocation} along with the CP node list.
 */
public class RaftInvocationContext {

    private final ILogger logger;
    private final RaftService raftService;
    private final ConcurrentMap<RaftGroupId, RaftMemberImpl> knownLeaders =
            new ConcurrentHashMap<RaftGroupId, RaftMemberImpl>();
    private final boolean failOnIndeterminateOperationState;

    private volatile RaftMemberImpl[] members = {};

    public RaftInvocationContext(ILogger logger, RaftService raftService) {
        this.logger = logger;
        this.raftService = raftService;
        this.failOnIndeterminateOperationState = raftService.getConfig().isFailOnIndeterminateOperationState();
    }

    public void reset() {
        members = new RaftMemberImpl[0];
        knownLeaders.clear();
    }

    public void setMembers(Collection<RaftMemberImpl> members) {
        this.members = members.toArray(new RaftMemberImpl[0]);
    }

    RaftMemberImpl getKnownLeader(RaftGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    boolean setKnownLeader(RaftGroupId groupId, RaftMemberImpl leader) {
        if (leader != null) {
            logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
            knownLeaders.put(groupId, leader);
            return true;
        }

        return false;
    }

    void updateKnownLeaderOnFailure(RaftGroupId groupId, Throwable cause) {
        if (cause instanceof RaftException) {
            RaftException e = (RaftException) cause;
            RaftMemberImpl leader = (RaftMemberImpl) e.getLeader();
            if (!setKnownLeader(groupId, leader)) {
                resetKnownLeader(groupId);
            }
        } else {
            resetKnownLeader(groupId);
        }
    }

    boolean shouldFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    private void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    MemberCursor newMemberCursor(RaftGroupId groupId) {
        RaftGroupInfo group = raftService.getRaftGroup(groupId);
        RaftMemberImpl[] endpoints = group != null ? group.membersArray() : members;
        return new MemberCursor(endpoints);
    }
}
