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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cp.internal.MetadataRaftGroupManager.INITIAL_METADATA_GROUP_ID;

/**
 *  Contains all static dependencies for a {@link RaftInvocation} along with the CP node list.
 */
public class RaftInvocationContext {

    private static final CPMembersContainer INITIAL_VALUE = new CPMembersContainer(
            new CPMembersVersion(INITIAL_METADATA_GROUP_ID.getSeed(), -1), new CPMemberInfo[0]);


    private final ILogger logger;
    private final RaftService raftService;
    private final ConcurrentMap<CPGroupId, CPMember> knownLeaders = new ConcurrentHashMap<>();
    private final boolean failOnIndeterminateOperationState;

    private AtomicReference<CPMembersContainer> membersContainer = new AtomicReference<>(INITIAL_VALUE);

    public RaftInvocationContext(ILogger logger, RaftService raftService) {
        this.logger = logger;
        this.raftService = raftService;
        this.failOnIndeterminateOperationState = raftService.getConfig().isFailOnIndeterminateOperationState();
    }

    public void reset() {
        membersContainer.set(INITIAL_VALUE);
        knownLeaders.clear();
    }

    public boolean setMembers(long groupIdSeed, long membersCommitIndex, Collection<? extends CPMember> members) {
        if (members.size() < 2) {
            return false;
        }

        CPMembersVersion version = new CPMembersVersion(groupIdSeed, membersCommitIndex);
        CPMembersContainer newContainer =  new CPMembersContainer(version, members.toArray(new CPMember[0]));
        while (true) {
            CPMembersContainer currentContainer = membersContainer.get();
            if (newContainer.version.compareTo(currentContainer.version) > 0) {
                if (membersContainer.compareAndSet(currentContainer, newContainer)) {
                    logger.fine("Replaced " + currentContainer + " with " + newContainer);
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public void updateMember(CPMember member) {
        while (true) {
            // Put the given member into the current member list,
            // even if the given member does not exist with another address.
            // In addition, remove any other member that has the address of the given member.
            CPMembersContainer currentContainer = membersContainer.get();
            CPMember otherMember = null;
            for (CPMember m : currentContainer.members) {
                if (m.getAddress().equals(member.getAddress()) && !m.getUuid().equals(member.getUuid())) {
                    otherMember = m;
                    break;
                }
            }
            CPMember existingMember = currentContainer.membersMap.get(member.getUuid());
            if (otherMember == null && existingMember != null && existingMember.getAddress().equals(member.getAddress())) {
                return;
            }

            Map<UUID, CPMember> newMembers = new HashMap<>(currentContainer.membersMap);
            newMembers.put(member.getUuid(), member);
            if (otherMember != null) {
                newMembers.remove(otherMember.getUuid());
            }

            CPMembersContainer newContainer = new CPMembersContainer(currentContainer.version, newMembers);
            if (membersContainer.compareAndSet(currentContainer, newContainer)) {
                logger.info("Replaced " + existingMember + " with " + member);
                return;
            }
        }
    }

    int getCPGroupPartitionId(CPGroupId groupId) {
        return raftService.getCPGroupPartitionId(groupId);
    }

    public CPMember getCPMember(UUID memberUid) {
        return membersContainer.get().membersMap.get(memberUid);
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
            if (!setKnownLeader(groupId, getCPMember(e.getLeaderUuid()))) {
                resetKnownLeader(groupId);
            }
        } else if (cause instanceof TargetNotMemberException
                || cause instanceof MemberLeftException
                || cause instanceof RetryableIOException) {
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

    MemberCursor newMemberCursor() {
        return new MemberCursor(membersContainer.get().members);
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

    private static class CPMembersContainer {
        final CPMembersVersion version;
        final CPMember[] members;
        final Map<UUID, CPMember> membersMap;

        CPMembersContainer(CPMembersVersion version, CPMember[] members) {
            this.version = version;
            this.members = members;
            membersMap = new HashMap<>(members.length);
            for (CPMember member : members) {
                membersMap.put(member.getUuid(), member);
            }
        }

        CPMembersContainer(CPMembersVersion version, Map<UUID, CPMember> members) {
            this.version = version;
            this.members = members.values().toArray(new CPMember[0]);
            this.membersMap = members;
        }

        @Override
        public String toString() {
            return "CPMembersContainer{" + "version=" + version + ", members=" + Arrays.toString(members) + '}';
        }
    }

    @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
    private static class CPMembersVersion implements Comparable<CPMembersVersion> {

        private final long groupIdSeed;
        private final long version;

        CPMembersVersion(long groupIdSeed, long version) {
            this.groupIdSeed = groupIdSeed;
            this.version = version;
        }

        @Override
        public int compareTo(@Nonnull CPMembersVersion other) {
            if (groupIdSeed < other.groupIdSeed) {
                return -1;
            } else if (groupIdSeed > other.groupIdSeed) {
                return 1;
            }

            return Long.compare(version, other.version);
        }

        @Override
        public String toString() {
            return "CPMembersVersion{" + "groupIdSeed=" + groupIdSeed + ", version=" + version + '}';
        }
    }
}
