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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SplitBrainHandler implements Runnable {
    final Node node;
    volatile long lastRun = 0;
    volatile boolean inProgress = false;

    public SplitBrainHandler(Node node) {
        this.node = node;
    }

    public void run() {
        if (node.isMaster()) {
            long now = System.currentTimeMillis();
            if (lastRun == 0) {
                lastRun = now + 5000;
            }
            if (!inProgress && (now - lastRun > 3000)) {
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
        Config config = node.getConfig();
        boolean multicastEnabled = config.getNetworkConfig().getJoin().getMulticastConfig().isEnabled();
        boolean tcpEnabled = config.getNetworkConfig().getJoin().getTcpIpConfig().isEnabled();
        if (multicastEnabled) {
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
                if (joinInfo != null) {
                    if (node.validateJoinRequest(joinInfo)) {
                        boolean shouldJoin = false;
                        int currentMemberCount = node.getClusterImpl().getMembers().size();
                        if (joinInfo.getMemberCount() > currentMemberCount) {
                            // I should join the other cluster
                            shouldJoin = true;
                        } else if (joinInfo.getMemberCount() == currentMemberCount) {
                            // compare the hashes
                            if (node.getThisAddress().hashCode() > joinInfo.address.hashCode()) {
                                shouldJoin = true;
                            }
                        }
                        if (shouldJoin) {
                            node.factory.restart();
                        }
                    }
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    } 
}
