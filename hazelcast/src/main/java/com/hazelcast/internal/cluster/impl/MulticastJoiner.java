/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.RandomPicker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MulticastJoiner extends AbstractJoiner {

    private static final long JOIN_RETRY_INTERVAL = 1000L;
    private static final int PUBLISH_INTERVAL_MIN = 50;
    private static final int PUBLISH_INTERVAL_MAX = 200;
    private static final int TRY_COUNT_MAX_LAST_DIGITS = 512;
    private static final int TRY_COUNT_MODULO = 10;

    private final AtomicInteger currentTryCount = new AtomicInteger(0);
    private final AtomicInteger maxTryCount;

    public MulticastJoiner(Node node) {
        super(node);
        maxTryCount = new AtomicInteger(calculateTryCount());
    }

    @Override
    public void doJoin() {
        long joinStartTime = Clock.currentTimeMillis();
        long maxJoinMillis = getMaxJoinMillis();
        Address thisAddress = node.getThisAddress();

        while (node.isRunning() && !node.joined()
                && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {

            // clear master node
            node.setMasterAddress(null);

            Address masterAddress = getTargetAddress();
            if (masterAddress == null) {
                masterAddress = findMasterWithMulticast();
            }
            node.setMasterAddress(masterAddress);

            if (masterAddress == null || thisAddress.equals(masterAddress)) {
                node.setAsMaster();
                return;
            }

            logger.info("Trying to join to discovered node: " + masterAddress);
            joinMaster();
        }
    }

    private void joinMaster() {
        long maxMasterJoinTime = getMaxJoinTimeToMasterNode();
        long start = Clock.currentTimeMillis();

        while (node.isRunning() && !node.joined()
                && Clock.currentTimeMillis() - start < maxMasterJoinTime) {

            Address master = node.getMasterAddress();
            if (master != null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Joining to master " + master);
                }
                clusterJoinManager.sendJoinRequest(master, true);
            } else {
                break;
            }

            try {
                Thread.sleep(JOIN_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }

            if (isBlacklisted(master)) {
                node.setMasterAddress(null);
                return;
            }
        }
    }

    @Override
    public void searchForOtherClusters() {
        final BlockingQueue<JoinMessage> q = new LinkedBlockingQueue<JoinMessage>();
        MulticastListener listener = new MulticastListener() {
            public void onMessage(Object msg) {
                if (msg != null && msg instanceof JoinMessage) {
                    JoinMessage joinRequest = (JoinMessage) msg;
                    if (node.getThisAddress() != null && !node.getThisAddress().equals(joinRequest.getAddress())) {
                        q.add(joinRequest);
                    }
                }
            }
        };
        node.multicastService.addMulticastListener(listener);
        node.multicastService.send(node.createJoinRequest(false));
        try {
            JoinMessage joinInfo = q.poll(3, TimeUnit.SECONDS);
            if (joinInfo != null) {
                if (node.clusterService.getMember(joinInfo.getAddress()) != null) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Ignoring merge join response, since " + joinInfo.getAddress()
                                + " is already a member.");
                    }
                    return;
                }

                if (joinInfo.getMemberCount() == 1) {
                    // if the other cluster has just single member, that may be a newly starting node instead of a split node
                    // wait 2 times 'WAIT_SECONDS_BEFORE_JOIN' seconds before processing merge JoinRequest
                    Thread.sleep(2 * node.getProperties().getMillis(GroupProperty.WAIT_SECONDS_BEFORE_JOIN));
                }

                JoinMessage response = sendSplitBrainJoinMessage(joinInfo.getAddress());
                if (shouldMerge(response)) {
                    logger.warning(node.getThisAddress() + " is merging [multicast] to " + joinInfo.getAddress());
                    startClusterMerge(joinInfo.getAddress());
                }
            }
        } catch (InterruptedException ignored) {
            EmptyStatement.ignore(ignored);
        } catch (Exception e) {
            if (logger != null) {
                logger.warning(e);
            }
        } finally {
            node.multicastService.removeMulticastListener(listener);
        }
    }

    @Override
    public String getType() {
        return "multicast";
    }

    void onReceivedJoinRequest(JoinRequest joinRequest) {
        if (joinRequest.getUuid().compareTo(node.getThisUuid()) < 0) {
            maxTryCount.incrementAndGet();
        }
    }

    private Address findMasterWithMulticast() {
        try {
            if (logger.isFineEnabled()) {
                logger.fine("Searching for master node. Max tries: " + maxTryCount.get());
            }
            JoinRequest joinRequest = node.createJoinRequest(false);
            while (node.isRunning() && currentTryCount.incrementAndGet() <= maxTryCount.get()) {
                joinRequest.setTryCount(currentTryCount.get());
                node.multicastService.send(joinRequest);
                if (node.getMasterAddress() == null) {
                    //noinspection BusyWait
                    Thread.sleep(getPublishInterval());
                } else {
                    return node.getMasterAddress();
                }
            }
        } catch (final Exception e) {
            if (logger != null) {
                logger.warning(e);
            }
        } finally {
            currentTryCount.set(0);
        }
        return null;
    }

    private int calculateTryCount() {
        final NetworkConfig networkConfig = config.getNetworkConfig();
        long timeoutMillis = TimeUnit.SECONDS.toMillis(networkConfig.getJoin().getMulticastConfig().getMulticastTimeoutSeconds());
        int avgPublishInterval = (PUBLISH_INTERVAL_MAX + PUBLISH_INTERVAL_MIN) / 2;
        int tryCount = (int) timeoutMillis / avgPublishInterval;
        String host = node.getThisAddress().getHost();
        int lastDigits;
        try {
            lastDigits = Integer.parseInt(host.substring(host.lastIndexOf('.') + 1));
        } catch (NumberFormatException e) {
            lastDigits = RandomPicker.getInt(TRY_COUNT_MAX_LAST_DIGITS);
        }
        int portDiff = node.getThisAddress().getPort() - networkConfig.getPort();
        tryCount += (lastDigits + portDiff) % TRY_COUNT_MODULO;
        return tryCount;
    }

    private int getPublishInterval() {
        return RandomPicker.getInt(PUBLISH_INTERVAL_MIN, PUBLISH_INTERVAL_MAX);
    }
}
