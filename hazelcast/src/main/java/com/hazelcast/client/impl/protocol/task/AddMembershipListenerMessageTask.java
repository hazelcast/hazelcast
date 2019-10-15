/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

public class AddMembershipListenerMessageTask
        extends AbstractCallableMessageTask<ClientAddMembershipListenerCodec.RequestParameters> {

    public AddMembershipListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        String serviceName = ClusterServiceImpl.SERVICE_NAME;
        ClusterServiceImpl service = getService(serviceName);
        boolean advancedNetworkConfigEnabled = isAdvancedNetworkEnabled();
        UUID registrationId = service.addMembershipListener(
                new MembershipListenerImpl(endpoint, advancedNetworkConfigEnabled));
        endpoint.addListenerDestroyAction(serviceName, serviceName, registrationId);
        return registrationId;
    }

    @Override
    protected ClientAddMembershipListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddMembershipListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddMembershipListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    public Permission getRequiredPermission() {
        return null;
    }

    private class MembershipListenerImpl
            implements InitialMembershipListener {
        private final ClientEndpoint endpoint;
        private final boolean advancedNetworkConfigEnabled;

        MembershipListenerImpl(ClientEndpoint endpoint, boolean advancedNetworkConfigEnabled) {
            this.endpoint = endpoint;
            this.advancedNetworkConfigEnabled = advancedNetworkConfigEnabled;
        }

        @Override
        public void init(InitialMembershipEvent membershipEvent) {
            ClusterService service = getService(ClusterServiceImpl.SERVICE_NAME);
            Collection<MemberImpl> members = service.getMemberImpls();
            Collection<Member> membersToSend = new ArrayList<>();
            for (MemberImpl member : members) {
                membersToSend.add(translateMemberAddress(member));
            }
            ClientMessage eventMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent(membersToSend);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();

            ClientMessage eventMessage =
                    ClientAddMembershipListenerCodec.encodeMemberEvent(translateMemberAddress(member),
                            MembershipEvent.MEMBER_ADDED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();
            ClientMessage eventMessage = ClientAddMembershipListenerCodec.encodeMemberEvent(translateMemberAddress(member),
                    MembershipEvent.MEMBER_REMOVED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberAttributeOperationType op = memberAttributeEvent.getOperationType();
            String key = memberAttributeEvent.getKey();
            String value = memberAttributeEvent.getValue() == null ? null : memberAttributeEvent.getValue().toString();
            ClientMessage eventMessage = ClientAddMembershipListenerCodec
                    .encodeMemberAttributeChangeEvent(memberAttributeEvent.getMember(), memberAttributeEvent.getMembers(), key,
                            op.getId(), value);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        private boolean shouldSendEvent() {
            if (!endpoint.isAlive()) {
                return false;
            }

            ClusterService clusterService = clientEngine.getClusterService();
            if (parameters.localOnly && !clusterService.isMaster()) {
                //if client registered localOnly, only master is allowed to send request
                return false;
            }
            return true;
        }

        // the member partition table that is sent out to clients must contain the addresses
        // on which cluster members listen for CLIENT protocol connections.
        // with advanced network config, we need to return Members whose getAddress method
        // returns the CLIENT server socket address
        private MemberImpl translateMemberAddress(MemberImpl member) {
            if (!advancedNetworkConfigEnabled) {
                return member;
            }

            Address clientAddress = member.getAddressMap().get(EndpointQualifier.CLIENT);

            MemberImpl result = new MemberImpl.Builder(clientAddress)
                    .version(member.getVersion())
                    .uuid(member.getUuid())
                    .localMember(member.localMember())
                    .liteMember(member.isLiteMember())
                    .memberListJoinVersion(member.getMemberListJoinVersion())
                    .attributes(member.getAttributes())
                    .build();
            return result;
        }
    }
}
