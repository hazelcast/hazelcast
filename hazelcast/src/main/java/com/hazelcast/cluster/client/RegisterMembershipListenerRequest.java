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

package com.hazelcast.cluster.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.ClientPortableHook;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;

import java.security.Permission;

public final class RegisterMembershipListenerRequest extends CallableClientRequest implements RetryableRequest {

    public RegisterMembershipListenerRequest() {
    }

    @Override
    public Object call() throws Exception {
        ClusterServiceImpl service = getService();
        ClientEndpoint endpoint = getEndpoint();
        String registrationId = service.addMembershipListener(new MembershipListenerImpl(endpoint));
        String name = ClusterServiceImpl.SERVICE_NAME;
        endpoint.setListenerRegistration(name, name, registrationId);
        return registrationId;
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.REGISTER_MEMBERSHIP_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    private class MembershipListenerImpl implements MembershipListener {
        private final ClientEndpoint endpoint;

        public MembershipListenerImpl(ClientEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            if (!endpoint.isAlive()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();
            ClientMembershipEvent event = new ClientMembershipEvent(member, MembershipEvent.MEMBER_ADDED);
            endpoint.sendEvent(null, event, getCallId());
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (!endpoint.isAlive()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();
            ClientMembershipEvent event = new ClientMembershipEvent(member, MembershipEvent.MEMBER_REMOVED);
            endpoint.sendEvent(null, event, getCallId());
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
            ClientMembershipEvent event = new ClientMembershipEvent(member, memberAttributeChange);
            endpoint.sendEvent(null, event, getCallId());
        }
    }
}
