/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.05.2018
 */
public class RaftInvocationContext {
    private final ILogger logger;

    private final RaftService raftService;
    private final ConcurrentMap<RaftGroupId, RaftMemberImpl> knownLeaders =
            new ConcurrentHashMap<RaftGroupId, RaftMemberImpl>();
    final boolean failOnIndeterminateOperationState;

    private volatile RaftMemberImpl[] allMembers = {};

    public RaftInvocationContext(ILogger logger, RaftService raftService) {
        this.logger = logger;
        this.raftService = raftService;
        this.failOnIndeterminateOperationState = raftService.getConfig()
                .getRaftAlgorithmConfig().isFailOnIndeterminateOperationState();
    }

    public void reset() {
        allMembers = new RaftMemberImpl[0];
        knownLeaders.clear();
    }

    public void setAllMembers(Collection<RaftMemberImpl> endpoints) {
        allMembers = endpoints.toArray(new RaftMemberImpl[0]);
    }

    public RaftMemberImpl getKnownLeader(RaftGroupId groupId) {
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

    private void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    MemberCursor newMemberCursor(RaftGroupId groupId) {
        RaftGroupInfo group = raftService.getRaftGroup(groupId);
        RaftMemberImpl[] endpoints = group != null ? group.membersArray() : allMembers;
        return new MemberCursor(endpoints);
    }

    static class MemberCursor {
        private final RaftMemberImpl[] members;
        private int index = -1;

        private MemberCursor(RaftMemberImpl[] members) {
            this.members = members;
        }

        boolean advance() {
            return ++index < members.length;
        }

        RaftMemberImpl get() {
            return members[index];
        }
    }
}
