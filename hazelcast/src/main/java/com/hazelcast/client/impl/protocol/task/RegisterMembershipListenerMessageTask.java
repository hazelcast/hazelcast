/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.RegisterMembershipListenerEventParameters;
import com.hazelcast.client.impl.protocol.parameters.RegisterMembershipListenerParameters;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;
import java.util.Collection;

public class RegisterMembershipListenerMessageTask extends AbstractCallableMessageTask<RegisterMembershipListenerParameters> {

    public RegisterMembershipListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        String serviceName = ClusterServiceImpl.SERVICE_NAME;
        ClusterServiceImpl service = getService(serviceName);
        ClientEndpoint endpoint = getEndpoint();
        String registrationId = service.addMembershipListener(new MembershipListenerImpl(endpoint));
        endpoint.setListenerRegistration(serviceName, serviceName, registrationId);
        ClientMessage clientMessage = AddListenerResultParameters.encode(registrationId);
        return clientMessage;
    }

    @Override
    protected RegisterMembershipListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return RegisterMembershipListenerParameters.decode(clientMessage);
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

    private class MembershipListenerImpl implements InitialMembershipListener {
        private final ClientEndpoint endpoint;

        public MembershipListenerImpl(ClientEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void init(InitialMembershipEvent membershipEvent) {
            ClusterService service = getService(ClusterServiceImpl.SERVICE_NAME);
            Collection<MemberImpl> memberList = service.getMemberList();
            ClientMessage eventMessage = RegisterMembershipListenerEventParameters.encode(memberList);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            if (!endpoint.isAlive()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();

            ClientMessage eventMessage = RegisterMembershipListenerEventParameters.encode(member,
                    RegisterMembershipListenerEventParameters.MEMBER_ADDED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (!endpoint.isAlive()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();
            ClientMessage eventMessage = RegisterMembershipListenerEventParameters.encode(member,
                    RegisterMembershipListenerEventParameters.MEMBER_REMOVED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            if (!endpoint.isAlive()) {
                return;
            }

            MemberImpl member = (MemberImpl) memberAttributeEvent.getMember();
            String uuid = member.getUuid();
            MemberAttributeOperationType op = memberAttributeEvent.getOperationType();
            String key = memberAttributeEvent.getKey();
            Object value = memberAttributeEvent.getValue();
            MemberAttributeChange memberAttributeChange = new MemberAttributeChange(uuid, op, key, value);
            ClientMessage eventMessage = RegisterMembershipListenerEventParameters.encode(member,
                    memberAttributeChange);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }
    }
}
