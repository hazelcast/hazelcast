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
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class MulticastJoiner extends AbstractJoiner {

    public MulticastJoiner(Node node) {
        super(node);
    }

    public void doJoin(AtomicBoolean joined) {
        int tryCount = 0;
        while (!joined.get()) {
            logger.log(Level.FINEST, "joining... " + node.getMasterAddress());
            Address masterAddressNow = findMasterWithMulticast();
            if (masterAddressNow != null && masterAddressNow.equals(node.getMasterAddress())) {
                tryCount--;
            }
            node.setMasterAddress(masterAddressNow);
            if (node.getMasterAddress() == null || node.address.equals(node.getMasterAddress())) {
                TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                if (tcpIpConfig != null && tcpIpConfig.isEnabled()) {
                    doTCP(joined);
                } else {
                    node.setAsMaster();
                }
                return;
            }
            if (tryCount++ > 22) {
                failedJoiningToMaster(true, tryCount);
            }
            if (!node.getMasterAddress().equals(node.address)) {
                connectAndSendJoinRequest(node.getMasterAddress());
            } else {
                node.setMasterAddress(null);
                tryCount = 0;
            }
            try {
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
            if (joinInfo != null) {
                if (shouldMerge(joinInfo)) {
                    logger.log(Level.WARNING, node.address + " is merging [multicast] to " + joinInfo.address);
                    node.factory.restart();
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

    private void connectAndSendJoinRequest(Address masterAddress) {
        if (masterAddress == null || masterAddress.equals(node.address)) {
            throw new IllegalArgumentException();
        }
        Connection conn = node.connectionManager.getOrConnect(masterAddress);
        logger.log(Level.FINEST, "Master connection " + conn);
        if (conn != null) {
            node.clusterManager.sendJoinRequest(masterAddress);
        }
    }

    private Address findMasterWithMulticast() {
        try {
            final String ip = System.getProperty("join.ip");
            if (ip == null) {
                JoinInfo joinInfo = node.createJoinInfo();
                int tryCount = config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastTimeoutSeconds() * 100;
                for (int i = 0; i < tryCount; i++) {
                    node.multicastService.send(joinInfo);
                    if (node.getMasterAddress() == null) {
                        Thread.sleep(10);
                    } else {
                        return node.getMasterAddress();
                    }
                }
            } else {
                logger.log(Level.FINEST, "RETURNING join.ip");
                return new Address(ip, config.getPort());
            }
        } catch (final Exception e) {
            if (logger != null) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
        return null;
    }
}
