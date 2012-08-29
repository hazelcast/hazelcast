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
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.Clock;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class MulticastJoiner extends AbstractJoiner {

    private final AtomicInteger currentTryCount = new AtomicInteger(0);
    private final AtomicInteger maxTryCount;

    public MulticastJoiner(Node node) {
        super(node);
        maxTryCount = new AtomicInteger(calculateTryCount());
    }

    public void doJoin(AtomicBoolean joined) {
        int tryCount = 0;
        long joinStartTime = Clock.currentTimeMillis();
        long maxJoinMillis = node.getGroupProperties().MAX_JOIN_SECONDS.getInteger() * 1000;
        
        while (node.isActive() && !joined.get() && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {
            String msg = "Joining to master node: " + node.getMasterAddress();
            logger.log(Level.FINEST, msg);
            systemLogService.logJoin(msg);
            
            final Address masterAddressNow;
            if (targetAddress == null) {
                masterAddressNow = findMasterWithMulticast();
            } else {
                // if target address is set explicitly, try to join target address first.
                masterAddressNow = targetAddress;
                targetAddress = null;
            }
            
            node.setMasterAddress(masterAddressNow);
            if (masterAddressNow != null) {
                systemLogService.logJoin("Setting master address to " + masterAddressNow);
            }
            if (node.getMasterAddress() == null || node.address.equals(node.getMasterAddress())) {
                TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                if (tcpIpConfig != null && tcpIpConfig.isEnabled()) {
                    doTCP(joined);
                } else {
                    node.setAsMaster();
                }
                return;
            }
            if (++tryCount > 49) {
                failedJoiningToMaster(true, tryCount);
            }
            if (!node.getMasterAddress().equals(node.address)) {
                connectAndSendJoinRequest(node.getMasterAddress());
            } else {
                node.setMasterAddress(null);
                tryCount = 0;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void doTCP(AtomicBoolean joined) {
        node.setMasterAddress(null);
        logger.log(Level.FINEST, "Multicast couldn't find cluster. Trying TCP/IP");
        new TcpIpJoiner(node).join(joined);
    }

    public void searchForOtherClusters(SplitBrainHandler splitBrainHandler) {
        final BlockingQueue q = new LinkedBlockingQueue();
        MulticastListener listener = new MulticastListener() {
            public void onMessage(Object msg) {
                systemLogService.logJoin("MulticastListener onMessage " + msg);
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
        systemLogService.logJoin("Sent multicast join request");
        try {
            JoinInfo joinInfo = (JoinInfo) q.poll(3, TimeUnit.SECONDS);
            if (joinInfo != null) {
                if (joinInfo.getMemberCount() == 1) {
                    // if the other cluster has just single member, that may be a newly starting node
                    // instead of a split node.
                    // Wait 2 times 'WAIT_SECONDS_BEFORE_JOIN' seconds before processing merge JoinInfo.
                    Thread.sleep(node.groupProperties.WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L * 2);
                }
                if (shouldMerge(joinInfo)) {
                    logger.log(Level.WARNING, node.address + " is merging [multicast] to " + joinInfo.address);
                    targetAddress = joinInfo.address;
                    node.clusterManager.sendClusterMergeToOthers(targetAddress);
                    splitBrainHandler.restart();
                    return;
                }
            }
        } catch (InterruptedException ignored) {
        } catch (Exception e) {
            if (logger != null) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        } finally {
            node.multicastService.removeMulticastListener(listener);
        }
    }

    private boolean connectAndSendJoinRequest(Address masterAddress) {
        if (masterAddress == null || masterAddress.equals(node.address)) {
            throw new IllegalArgumentException();
        }
        Connection conn = node.connectionManager.getOrConnect(masterAddress);
        logger.log(Level.FINEST, "Master connection " + conn);
        systemLogService.logJoin("Master connection " + conn);
        if (conn != null) {
            return node.clusterManager.sendJoinRequest(masterAddress, true);
        } else {
            logger.log(Level.INFO, "Connecting to master node: " + masterAddress);
            return false;
        }
    }

    private Address findMasterWithMulticast() {
        try {
            final String ip = System.getProperty("join.ip");
            if (ip == null) {
                JoinInfo joinInfo = node.createJoinInfo();
                for (; node.isActive() && currentTryCount.incrementAndGet() <= maxTryCount.get(); ) {
                    joinInfo.setTryCount(currentTryCount.get());
                    node.multicastService.send(joinInfo);
                    if (node.getMasterAddress() == null) {
                        //noinspection BusyWait
                        Thread.sleep(10);
                    } else {
                        return node.getMasterAddress();
                    }
                }
            } else {
                logger.log(Level.FINEST, "RETURNING join.ip");
                return new Address(ip, config.getNetworkConfig().getPort());
            }
        } catch (final Exception e) {
            if (logger != null) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        } finally {
            this.currentTryCount.set(0);
        }
        return null;
    }

    private int calculateTryCount() {
        final NetworkConfig networkConfig = config.getNetworkConfig();
        int timeoutSeconds = networkConfig.getJoin().getMulticastConfig().getMulticastTimeoutSeconds();
        int tryCount = timeoutSeconds * 100;
        String host = node.address.getHost();
        int lastDigits = 0;
        try {
            lastDigits = Integer.valueOf(host.substring(host.lastIndexOf(".") + 1));
        } catch (NumberFormatException e) {
            lastDigits = (int) (512 * Math.random());
        }
        lastDigits = lastDigits % 100;
        tryCount += lastDigits + (node.address.getPort() - networkConfig.getPort()) * timeoutSeconds * 3;
        return tryCount;
    }

    public void onReceivedJoinInfo(JoinInfo joinInfo) {
        if (joinInfo.getTryCount() > this.currentTryCount.get() + 20) {
            int timeoutSeconds = (config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastTimeoutSeconds() + 4) * 100;
            this.maxTryCount.set(timeoutSeconds);
        }
    }
}
