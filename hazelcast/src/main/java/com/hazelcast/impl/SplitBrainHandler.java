/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.config.Config;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class SplitBrainHandler implements Runnable {
    final Node node;
    final ILogger logger;
    volatile long lastRun = 0;
    volatile boolean inProgress = false;
    final long FIRST_RUN_DELAY_MILLIS;
    final long NEXT_RUN_DELAY_MILLIS;
    final boolean multicastEnabled;
    final boolean tcpEnabled;

    public SplitBrainHandler(Node node) {
        this.node = node;
        this.logger = node.getLogger(SplitBrainHandler.class.getName());
        FIRST_RUN_DELAY_MILLIS = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong() * 1000L;
        NEXT_RUN_DELAY_MILLIS = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong() * 1000L;
        Config config = node.getConfig();
        multicastEnabled = config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled();
        tcpEnabled = config.getNetworkConfig().getJoin().getTcpIpConfig().isEnabled();
        lastRun = System.currentTimeMillis() + FIRST_RUN_DELAY_MILLIS;
    }

    public void run() {
        if (node.isMaster() && node.joined() && node.isActive()) {
            long now = System.currentTimeMillis();
            if (!inProgress && (now - lastRun > NEXT_RUN_DELAY_MILLIS) && node.clusterManager.shouldTryMerge()) {
                inProgress = true;
                node.executorManager.executeNow(new Runnable() {
                    public void run() {
                        searchForOtherClusters();
                        lastRun = System.currentTimeMillis();
                        inProgress = false;
                    }
                });
            }
        }
    }

    private void searchForOtherClusters() {
        if (multicastEnabled && node.multicastService != null) {
            final BlockingQueue q = new LinkedBlockingQueue();
            MulticastListener listener = new MulticastListener() {
                public void onMessage(Object msg) {
                    if (msg != null && msg instanceof JoinInfo) {
                        JoinInfo joinInfo = (JoinInfo) msg;
                        if (node.address != null && !node.address.equals(joinInfo.address)) {
                            q.offer(msg);
                        }
                    }
                }
            };
            node.multicastService.addMulticastListener(listener);
            node.multicastService.send(node.createJoinInfo());
            try {
                JoinInfo joinInfo = (JoinInfo) q.poll(3, TimeUnit.SECONDS);
                node.multicastService.removeMulticastListener(listener);
                if (shouldMerge(joinInfo)) {
                    node.factory.restart();
                    return;
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (tcpEnabled) {
            final Collection<Address> colPossibleAddresses = node.getPossibleMembers();
            colPossibleAddresses.remove(node.getThisAddress());
            for (Member member : node.getClusterImpl().getMembers()) {
                colPossibleAddresses.remove(((MemberImpl) member).getAddress());
            }
            if (colPossibleAddresses.size() == 0) {
                return;
            }
            for (final Address possibleAddress : colPossibleAddresses) {
                logger.log(Level.FINEST, node.getThisAddress() + " is connecting to " + possibleAddress);
                node.connectionManager.getOrConnect(possibleAddress);
            }
            for (Address possibleAddress : colPossibleAddresses) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
                final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
                if (conn != null) {
                    JoinInfo response = node.clusterManager.checkJoin(conn);
                    if (shouldMerge(response)) {
                        // we will join so delay the merge checks.
                        lastRun = System.currentTimeMillis() + FIRST_RUN_DELAY_MILLIS;
                        node.factory.restart();
                    }
                    // trying one live connection is good enough
                    // no need to try other connections
                    return;
                }
            }
        }
    }

    boolean shouldMerge(JoinInfo joinInfo) {
        boolean shouldMerge = false;
        if (joinInfo != null) {
            boolean validJoinRequest;
            try {
                validJoinRequest = node.validateJoinRequest(joinInfo);
            } catch (Exception e) {
                validJoinRequest = false;
            }
            if (validJoinRequest){
                int currentMemberCount = node.getClusterImpl().getMembers().size();
                if (joinInfo.getMemberCount() > currentMemberCount) {
                    // I should join the other cluster
                    shouldMerge = true;
                } else if (joinInfo.getMemberCount() == currentMemberCount) {
                    // compare the hashes
                    if (node.getThisAddress().hashCode() > joinInfo.address.hashCode()) {
                        shouldMerge = true;
                    }
                }
            }
        }
        return shouldMerge;
    }
}
