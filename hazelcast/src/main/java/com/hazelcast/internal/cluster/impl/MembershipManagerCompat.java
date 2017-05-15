/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;

import static java.lang.String.format;

/**
 * Implements member removal logic used before 3.9
 */
@Deprecated
public class MembershipManagerCompat {

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final MembershipManager membershipManager;
    private final Lock clusterServiceLock;
    private final ILogger logger;

    MembershipManagerCompat(Node node, ClusterServiceImpl clusterService, Lock clusterServiceLock) {
        this.node = node;
        this.clusterService = clusterService;
        this.clusterServiceLock = clusterServiceLock;

        this.nodeEngine = node.getNodeEngine();
        this.membershipManager = clusterService.getMembershipManager();
        this.logger = node.getLogger(getClass());
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public void removeMember(Address deadAddress, String uuid, String reason) {
        if (clusterService.getClusterVersion().isGreaterOrEqual(Versions.V3_9)) {
            throw new IllegalStateException("Should not be called on versions 3.9+");
        }

        if (!ensureMemberIsRemovable(deadAddress)) {
            return;
        }

        clusterServiceLock.lock();
        try {
            if (!clusterService.isJoined()) {
                logger.fine("Cannot remove " + deadAddress + " with uuid: " + uuid + " because this node is not joined...");
                return;
            }

            MemberImpl member = membershipManager.getMember(deadAddress);
            if (member == null || (uuid != null && !uuid.equals(member.getUuid()))) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot remove " + deadAddress + ", either member is not present "
                            + "or uuid is not matching. Uuid: " + uuid + ", member: " + member);
                }
                return;
            }

            if (deadAddress.equals(clusterService.getMasterAddress())) {
                assignNewMaster();
            }
            if (clusterService.isMaster()) {
                clusterService.getClusterJoinManager().removeJoin(deadAddress);
            }
            Connection conn = node.connectionManager.getConnection(deadAddress);
            if (conn != null) {
                conn.close(reason, null);
            }

            removeMember(member);
            clusterService.printMemberList();

        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void assignNewMaster() {
        Address oldMasterAddress = clusterService.getMasterAddress();
        if (clusterService.isJoined()) {
            Collection<MemberImpl> members = membershipManager.getMembers();
            Member newMaster = null;
            int size = members.size();
            if (size > 1) {
                Iterator<MemberImpl> iterator = members.iterator();
                Member member = iterator.next();
                if (member.getAddress().equals(oldMasterAddress)) {
                    newMaster = iterator.next();
                } else {
                    logger.severe(format("Old master %s is dead, but the first of member list is a different member %s!",
                            oldMasterAddress, member));
                    newMaster = member;
                }
            } else {
                logger.warning(format("Old master %s is dead and this node is not master, "
                        + "but member list contains only %d members: %s", oldMasterAddress, size, members));
            }
            logger.info(format("Old master %s left the cluster, assigning new master %s", oldMasterAddress, newMaster));
            if (newMaster != null) {
                clusterService.setMasterAddress(newMaster.getAddress());
            } else {
                clusterService.setMasterAddress(null);
            }
        } else {
            clusterService.setMasterAddress(null);
        }

        if (logger.isFineEnabled()) {
            logger.fine(format("Old master: %s, new master: %s ", oldMasterAddress, clusterService.getMasterAddress()));
        }

        ClusterHeartbeatManager clusterHeartbeatManager = clusterService.getClusterHeartbeatManager();
        if (clusterService.isMaster()) {
            clusterHeartbeatManager.resetMemberMasterConfirmations();
        } else {
            clusterHeartbeatManager.sendMasterConfirmation();
        }
    }

    private void removeMember(MemberImpl deadMember) {
        assert clusterService.getClusterVersion().isLessThan(Versions.V3_9);

        logger.info("Removing " + deadMember);
        clusterServiceLock.lock();
        try {
            ClusterHeartbeatManager clusterHeartbeatManager = clusterService.getClusterHeartbeatManager();
            MemberMap currentMembers = membershipManager.getMemberMap();
            if (currentMembers.contains(deadMember.getAddress())) {
                clusterHeartbeatManager.removeMember(deadMember);
                MemberMap newMembers = MemberMap.cloneExcluding(currentMembers, deadMember);
                membershipManager.setMembers(newMembers);

                if (clusterService.isMaster()) {
                    if (logger.isFineEnabled()) {
                        logger.fine(deadMember + " is dead, sending remove to all other members...");
                    }

                    sendMemberRemoveOperation(deadMember);
                }

                membershipManager.handleMemberRemove(newMembers, deadMember);
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void sendMemberRemoveOperation(Member deadMember) {
        for (Member member : membershipManager.getMembers()) {
            Address address = member.getAddress();
            if (!node.getThisAddress().equals(address) && !address.equals(deadMember.getAddress())) {
                MemberRemoveOperation
                        op = new MemberRemoveOperation(deadMember.getAddress(), deadMember.getUuid());
                nodeEngine.getOperationService().send(op, address);
            }
        }
    }

    private boolean ensureMemberIsRemovable(Address deadAddress) {
        return clusterService.isJoined() && !deadAddress.equals(node.getThisAddress());
    }

}
