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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.Connection;

import java.security.Permission;
import java.util.Collection;

public class AddMembershipListenerMessageTask
        extends AbstractCallableMessageTask<ClientAddMembershipListenerCodec.RequestParameters> {

    public AddMembershipListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        String serviceName = ClusterServiceImpl.SERVICE_NAME;
        ClusterServiceImpl service = getService(serviceName);
        ClientEndpoint endpoint = getEndpoint();
        String registrationId = service.addMembershipListener(new MembershipListenerImpl(endpoint));
        endpoint.addListenerDestroyAction(serviceName, serviceName, registrationId);
        return registrationId;
    }

    @Override
    protected ClientAddMembershipListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddMembershipListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddMembershipListenerCodec.encodeResponse((String) response);
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

        public MembershipListenerImpl(ClientEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void init(InitialMembershipEvent membershipEvent) {
            ClusterService service = getService(ClusterServiceImpl.SERVICE_NAME);
            Collection members = service.getMemberImpls();
            ClientMessage eventMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent(members);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();

            ClientMessage eventMessage =
                    ClientAddMembershipListenerCodec.encodeMemberEvent(member, MembershipEvent.MEMBER_ADDED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();
            ClientMessage eventMessage =
                    ClientAddMembershipListenerCodec.encodeMemberEvent(member, MembershipEvent.MEMBER_REMOVED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) memberAttributeEvent.getMember();
            String uuid = member.getUuid();
            MemberAttributeOperationType op = memberAttributeEvent.getOperationType();
            String key = memberAttributeEvent.getKey();
            String value = memberAttributeEvent.getValue() == null ? null : memberAttributeEvent.getValue().toString();
            ClientMessage eventMessage =
                    ClientAddMembershipListenerCodec.encodeMemberAttributeChangeEvent(uuid, key, op.getId(), value);
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
    }
}
