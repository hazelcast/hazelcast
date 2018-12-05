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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.CPGroupInfo;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *  Contains all static dependencies for a {@link RaftInvocation} along with the CP node list.
 */
public class RaftInvocationContext {

    private final ILogger logger;
    private final RaftService raftService;
    private final ConcurrentMap<CPGroupId, CPMember> knownLeaders =
            new ConcurrentHashMap<CPGroupId, CPMember>();
    private final boolean failOnIndeterminateOperationState;

    private volatile CPMemberInfo[] members = {};

    public RaftInvocationContext(ILogger logger, RaftService raftService) {
        this.logger = logger;
        this.raftService = raftService;
        this.failOnIndeterminateOperationState = raftService.getConfig().isFailOnIndeterminateOperationState();
    }

    public void reset() {
        members = new CPMemberInfo[0];
        knownLeaders.clear();
    }

    public void setMembers(Collection<CPMemberInfo> members) {
        this.members = members.toArray(new CPMemberInfo[0]);
    }

    CPMember getKnownLeader(CPGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    boolean setKnownLeader(CPGroupId groupId, CPMember leader) {
        if (leader != null) {
            logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
            knownLeaders.put(groupId, leader);
            return true;
        }

        return false;
    }

    void updateKnownLeaderOnFailure(CPGroupId groupId, Throwable cause) {
        if (cause instanceof CPSubsystemException) {
            CPSubsystemException e = (CPSubsystemException) cause;
            CPMember leader = (CPMember) e.getLeader();
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

    private void resetKnownLeader(CPGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    MemberCursor newMemberCursor(CPGroupId groupId) {
        CPGroupInfo group = raftService.getCPGroupLocally(groupId);
        CPMember[] endpoints = group != null ? group.membersArray() : members;
        return new MemberCursor(endpoints);
    }
}
