/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.config.Config;
import com.hazelcast.core.Member;
import com.hazelcast.impl.base.SystemLogService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public abstract class AbstractJoiner implements Joiner {
    private final long joinStartTime = Clock.currentTimeMillis();
    protected final Config config;
    protected final Node node;
    protected volatile ILogger logger;
    private final AtomicInteger tryCount = new AtomicInteger(0);
    protected final SystemLogService systemLogService;

    public AbstractJoiner(Node node) {
        this.node = node;
        this.systemLogService = node.getSystemLogService();
        if (node.loggingService != null) {
            this.logger = node.loggingService.getLogger(this.getClass().getName());
        }
        this.config = node.config;
    }

    public abstract void doJoin(AtomicBoolean joined);

    public void join(AtomicBoolean joined) {
        doJoin(joined);
        postJoin();
    }

    private void postJoin() {
        systemLogService.logJoin("PostJoin master: " + node.getMasterAddress() + ", isMaster: " + node.isMaster());
        if (!node.isActive()) {
            return;
        }
        if (tryCount.incrementAndGet() == 5) {
            logger.log(Level.WARNING, "Join try count exceed limit, setting this node as master!");
            node.setAsMaster();
        }
        if (!node.isMaster()) {
            boolean allConnected = false;
            int checkCount = 0;
            long maxJoinMillis = node.getGroupProperties().MAX_JOIN_SECONDS.getInteger() * 1000;
            if (node.joined()) {
                systemLogService.logJoin("Waiting for all connections");
                while (checkCount++ < node.groupProperties.CONNECT_ALL_WAIT_SECONDS.getInteger() && !allConnected) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                    Set<Member> members = node.getClusterImpl().getMembers();
                    allConnected = true;
                    for (Member member : members) {
                        MemberImpl memberImpl = (MemberImpl) member;
                        if (!memberImpl.localMember() && node.connectionManager.getOrConnect(memberImpl.getAddress()) == null) {
                            allConnected = false;
                            systemLogService.logJoin("Not-connected to " + memberImpl.getAddress());
                        }
                    }
                }
            }
            if (!node.joined() || !allConnected) {
                if (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis) {
                    logger.log(Level.WARNING, "Failed to connect, node joined= " + node.joined() + ", allConnected= " + allConnected + " to all other members after " + checkCount + " seconds.");
                    logger.log(Level.WARNING, "Rebooting after 10 seconds.");
                    try {
                        Thread.sleep(10000);
                        node.rejoin();
                    } catch (InterruptedException e) {
                        logger.log(Level.WARNING, e.getMessage(), e);
                        node.shutdown(false, true);
                    }
                } else {
                    throw new RuntimeException("Failed to join in " + (maxJoinMillis / 1000) + " seconds!");
                }
                return;
            } else {
                node.clusterImpl.finalizeJoin();
            }
        }

        tryCount.set(0);
//        node.clusterManager.enqueueAndWait(new Processable() {
//            public void process() {
//                if (node.baseVariables.lsMembers.size() == 1) {
//                    final StringBuilder sb = new StringBuilder();
//                    sb.append("\n");
//                    sb.append(node.clusterManager);
//                    logger.log(Level.INFO, sb.toString());
//                }
//            }
//        }, 5);
        if (node.getClusterImpl().getSize() == 1) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append(node.clusterImpl);
            logger.log(Level.INFO, sb.toString());
        }
    }

    protected void failedJoiningToMaster(boolean multicast, int tryCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("======================================================");
        sb.append("\n");
        sb.append("Couldn't connect to discovered master! tryCount: ").append(tryCount);
        sb.append("\n");
        sb.append("address: ").append(node.address);
        sb.append("\n");
        sb.append("masterAddress: ").append(node.getMasterAddress());
        sb.append("\n");
        sb.append("multicast: ").append(multicast);
        sb.append("\n");
        sb.append("connection: ").append(node.connectionManager.getConnection(node.getMasterAddress()));
        sb.append("\n");
        sb.append("======================================================");
        sb.append("\n");
        throw new IllegalStateException(sb.toString());
    }

    boolean shouldMerge(JoinInfo joinInfo) {
        boolean shouldMerge = false;
        if (joinInfo != null) {
            boolean validJoinRequest;
            try {
                try {
                    validJoinRequest = node.validateJoinRequest(joinInfo);
                } catch (Exception e) {
                    logger.log(Level.FINEST, e.getMessage());
                    validJoinRequest = false;
                }
                if (validJoinRequest) {
                    for (Member member : node.getClusterImpl().getMembers()) {
                        MemberImpl memberImpl = (MemberImpl) member;
                        if (memberImpl.getAddress().equals(joinInfo.address)) {
                            logger.log(Level.FINEST, "Should not merge to " + joinInfo.address
                                      + ", because it is already member of this cluster.");
                            return false;
                        }
                    }
                    int currentMemberCount = node.getClusterImpl().getMembers().size();
                    if (joinInfo.getMemberCount() > currentMemberCount) {
                        // I should join the other cluster
                        logger.log(Level.INFO, node.address + " is merging to " + joinInfo.address
                                                 + ", because : joinInfo.getMemberCount() > currentMemberCount ["
                                                 + (joinInfo.getMemberCount() + " > " + currentMemberCount) + "]");
                        logger.log(Level.FINEST, joinInfo.toString());
                        shouldMerge = true;
                    } else if (joinInfo.getMemberCount() == currentMemberCount) {
                        // compare the hashes
                        if (node.getThisAddress().hashCode() > joinInfo.address.hashCode()) {
                            logger.log(Level.INFO, node.address + " is merging to " + joinInfo.address
                                                     + ", because : node.getThisAddress().hashCode() > joinInfo.address.hashCode() "
                                                     + ", this node member count: " + currentMemberCount);
                            logger.log(Level.FINEST, joinInfo.toString());
                            shouldMerge = true;
                        } else {
                            logger.log(Level.FINEST, joinInfo.address + " should merge to this node "
                                                     + ", because : node.getThisAddress().hashCode() < joinInfo.address.hashCode() "
                                                     + ", this node member count: " + currentMemberCount);
                        }
                    }
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                return false;
            }
        }
        return shouldMerge;
    }

    protected void connectAndSendJoinRequest(Collection<Address> colPossibleAddresses) {
        if (node.getFailedConnections().size() > 0)
            for (Address possibleAddress : colPossibleAddresses) {
                final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
                if (conn != null) {
                    logger.log(Level.FINEST, "sending join request for " + possibleAddress);
                    node.clusterImpl.sendJoinRequest(possibleAddress, true);
                }
            }
    }

    public final long getStartTime() {
        return joinStartTime;
    }
}
