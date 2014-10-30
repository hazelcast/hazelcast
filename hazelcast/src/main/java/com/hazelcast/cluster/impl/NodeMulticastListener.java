/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import java.util.Set;

import static com.hazelcast.util.AddressUtil.matchAnyInterface;
import static java.lang.String.format;

public class NodeMulticastListener implements MulticastListener {

    private final Node node;
    private final Set<String> trustedInterfaces;
    private final ILogger logger;
    private ConfigCheck ourConfig;

    public NodeMulticastListener(Node node) {
        this.node = node;
        this.trustedInterfaces = node.getConfig().getNetworkConfig()
                .getJoin().getMulticastConfig().getTrustedInterfaces();
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
        if (node.isActive() && node.joined()) {
            handleActiveAndJoined(joinMessage);
        } else {
            handleNotActiveOrNotJoined(joinMessage);
        }
    }

    private void logDroppedMessage(Object msg) {
        if (logger.isFinestEnabled()) {
            logger.finest("Dropped: " + msg);
        }
    }

    private void handleActiveAndJoined(JoinMessage joinMessage) {
        if (!(joinMessage instanceof JoinRequest)) {
            logDroppedMessage(joinMessage);
            return;
        }

        if (node.isMaster()) {
            JoinRequest request = (JoinRequest) joinMessage;
            JoinMessage response = new JoinMessage(request.getPacketVersion(), request.getBuildNumber(),
                    node.getThisAddress(), request.getUuid(), request.getConfigCheck(),
                    node.getClusterService().getSize());
            node.multicastService.send(response);
        } else if (isMasterNode(joinMessage.getAddress()) && !checkMasterUuid(joinMessage.getUuid())) {
            logger.warning("New join request has been received from current master. "
                    + "Removing " + node.getMasterAddress());
            node.getClusterService().removeAddress(node.getMasterAddress());
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

            if (!node.joined() && node.getMasterAddress() == null) {
                String masterHost = joinMessage.getAddress().getHost();
                if (trustedInterfaces.isEmpty() || matchAnyInterface(masterHost, trustedInterfaces)) {
                    //todo: why are we making a copy here of address?
                    Address masterAddress = new Address(joinMessage.getAddress());
                    node.setMasterAddress(masterAddress);
                } else {
                    logJoinMessageDropped(masterHost);
                }
            } else {
                logDroppedMessage(joinMessage);
            }
        }
    }

    private boolean isJoinRequest(JoinMessage joinMessage) {
        return joinMessage instanceof JoinRequest;
    }

    private void logJoinMessageDropped(String masterHost) {
        if (logger.isFinestEnabled()) {
            logger.finest(format(
                    "JoinMessage from %s is dropped because its sender is not a trusted interface", masterHost));
        }
    }

    private boolean isJoinMessage(Object msg) {
        return msg != null && msg instanceof JoinMessage;
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

    private boolean isMasterNode(Address address) {
        return address.equals(node.getMasterAddress());
    }

    private boolean checkMasterUuid(String uuid) {
        Member masterMember = getMasterMember(node.getClusterService().getMembers());
        return masterMember == null || masterMember.getUuid().equals(uuid);
    }

    private Member getMasterMember(Set<Member> members) {
        if (members.isEmpty()) {
            return null;
        }

        return members.iterator().next();
    }
}
