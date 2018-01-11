/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Joiner;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

public class NodeMulticastListener implements MulticastListener {

    private final Node node;
    private final ILogger logger;
    private ConfigCheck ourConfig;

    public NodeMulticastListener(Node node) {
        this.node = node;
        this.logger = node.getLogger(NodeMulticastListener.class.getName());
        this.ourConfig = node.createConfigCheck();
    }

    @Override
    public void onMessage(Object msg) {
        if (!isValidJoinMessage(msg)) {
            logDroppedMessage(msg);
            return;
        }

        JoinMessage joinMessage = (JoinMessage) msg;
        if (node.isRunning() && node.getClusterService().isJoined()) {
            handleActiveAndJoined(joinMessage);
        } else {
            handleNotActiveOrNotJoined(joinMessage);
        }
    }

    private void logDroppedMessage(Object msg) {
        if (logger.isFineEnabled()) {
            logger.fine("Dropped: " + msg);
        }
    }

    private void handleActiveAndJoined(JoinMessage joinMessage) {
        if (!(joinMessage instanceof JoinRequest)) {
            logDroppedMessage(joinMessage);
            return;
        }

        ClusterServiceImpl clusterService = node.getClusterService();
        Address masterAddress = clusterService.getMasterAddress();
        if (clusterService.isMaster()) {
            JoinMessage response = new JoinMessage(Packet.VERSION, node.getBuildInfo().getBuildNumber(), node.getVersion(),
                    node.getThisAddress(), node.getThisUuid(), node.isLiteMember(), node.createConfigCheck());
            node.multicastService.send(response);
        } else if (joinMessage.getAddress().equals(masterAddress)) {
            MemberImpl master = node.getClusterService().getMember(masterAddress);
            if (master != null && !master.getUuid().equals(joinMessage.getUuid())) {
                String message = "New join request has been received from current master. Suspecting " + masterAddress;
                logger.warning(message);
                // I just make a local suspicion. Probably other nodes will eventually suspect as well.
                clusterService.suspectMember(master, message, false);
            }
        }
    }

    private void handleNotActiveOrNotJoined(JoinMessage joinMessage) {
        if (isJoinRequest(joinMessage)) {
            Joiner joiner = node.getJoiner();
            if (joiner instanceof MulticastJoiner) {
                MulticastJoiner multicastJoiner = (MulticastJoiner) joiner;
                multicastJoiner.onReceivedJoinRequest((JoinRequest) joinMessage);
            } else {
                logDroppedMessage(joinMessage);
            }
        } else {
            Address address = joinMessage.getAddress();
            if (node.getJoiner().isBlacklisted(address)) {
                logDroppedMessage(joinMessage);
                return;
            }

            ClusterServiceImpl clusterService = node.getClusterService();
            if (!clusterService.isJoined() && clusterService.getMasterAddress() == null) {
                clusterService.setMasterAddressToJoin(joinMessage.getAddress());
            } else {
                logDroppedMessage(joinMessage);
            }
        }
    }

    private boolean isJoinRequest(JoinMessage joinMessage) {
        return joinMessage instanceof JoinRequest;
    }

    private boolean isJoinMessage(Object msg) {
        return msg != null && msg instanceof JoinMessage && !(msg instanceof SplitBrainJoinMessage);
    }

    private boolean isValidJoinMessage(Object msg) {
        if (!isJoinMessage(msg)) {
            return false;
        }

        JoinMessage joinMessage = (JoinMessage) msg;

        if (isMessageToSelf(joinMessage)) {
            return false;
        }

        ConfigCheck theirConfig = joinMessage.getConfigCheck();
        if (!ourConfig.isSameGroup(theirConfig)) {
            return false;
        }
        return true;
    }

    private boolean isMessageToSelf(JoinMessage joinMessage) {
        Address thisAddress = node.getThisAddress();
        return thisAddress == null || thisAddress.equals(joinMessage.getAddress());
    }
}
