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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 *  Contains all static dependencies for a {@link RaftInvocation} along with the CP node list.
 */
public class RaftInvocationContext {

    private final ILogger logger;
    private final RaftService raftService;
    private final ConcurrentMap<CPGroupId, CPMember> knownLeaders =
            new ConcurrentHashMap<CPGroupId, CPMember>();
    private final boolean failOnIndeterminateOperationState;

    private AtomicReference<ActiveCPMembersContainer> membersContainer = new AtomicReference<ActiveCPMembersContainer>(null);

    public RaftInvocationContext(ILogger logger, RaftService raftService) {
        this.logger = logger;
        this.raftService = raftService;
        this.failOnIndeterminateOperationState = raftService.getConfig().isFailOnIndeterminateOperationState();
    }

    public void reset() {
        membersContainer.set(null);
        knownLeaders.clear();
    }

    public void setMembers(long groupIdSeed, long membersCommitIndex, Collection<CPMemberInfo> members) {
        ActiveCPMembersVersion version = new ActiveCPMembersVersion(groupIdSeed, membersCommitIndex);
        ActiveCPMembersContainer newContainer =  new ActiveCPMembersContainer(version, members.toArray(new CPMemberInfo[0]));
        while (true) {
            ActiveCPMembersContainer currentContainer = membersContainer.get();
            if (currentContainer == null || newContainer.version.compareTo(currentContainer.version) > 0) {
                if (membersContainer.compareAndSet(currentContainer, newContainer)) {
                    return;
                }
            } else {
                return;
            }
        }
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
        if (group != null) {
            return new MemberCursor(group.membersArray());
        }

        ActiveCPMembersContainer container = membersContainer.get();
        CPMember[] members = container != null ? container.members : new CPMember[0];
        return new MemberCursor(members);
    }

    /**
     * Iterates over Raft members
     */
    static final class MemberCursor {
        private final CPMember[] members;
        private int index = -1;

        MemberCursor(CPMember[] members) {
            this.members = members;
        }

        boolean advance() {
            return ++index < members.length;
        }

        CPMember get() {
            return members[index];
        }
    }

    private static class ActiveCPMembersContainer {
        final ActiveCPMembersVersion version;
        final CPMemberInfo[] members;

        ActiveCPMembersContainer(ActiveCPMembersVersion version, CPMemberInfo[] members) {
            this.version = version;
            this.members = members;
        }
    }

    @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
    private static class ActiveCPMembersVersion implements Comparable<ActiveCPMembersVersion> {

        private final long groupIdSeed;
        private final long version;

        ActiveCPMembersVersion(long groupIdSeed, long version) {
            this.groupIdSeed = groupIdSeed;
            this.version = version;
        }

        @Override
        public int compareTo(@Nonnull ActiveCPMembersVersion other) {
            if (groupIdSeed < other.groupIdSeed) {
                return -1;
            } else if (groupIdSeed > other.groupIdSeed) {
                return 1;
            }

            return version < other.version ? -1 : (version > other.version ? 1 : 0);
        }
    }
}
