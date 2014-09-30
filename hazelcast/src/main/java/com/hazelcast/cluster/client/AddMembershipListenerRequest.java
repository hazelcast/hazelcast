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

package com.hazelcast.cluster.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.ClientPortableHook;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializableCollection;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;

public final class AddMembershipListenerRequest extends CallableClientRequest implements RetryableRequest {

    public AddMembershipListenerRequest() {
    }

    @Override
    public Object call() throws Exception {
        final ClusterServiceImpl service = getService();
        final ClientEndpoint endpoint = getEndpoint();

        final String registrationId = service.addMembershipListener(new MembershipListenerImpl(endpoint));

        final String name = ClusterServiceImpl.SERVICE_NAME;
        endpoint.setListenerRegistration(name, name, registrationId);

        final Collection<MemberImpl> memberList = service.getMemberList();
        final Collection<Data> response = new ArrayList<Data>(memberList.size());
        for (MemberImpl member : memberList) {
            response.add(serializationService.toData(member));
        }
        return new SerializableCollection(response);
    }

    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    public int getClassId() {
        return ClientPortableHook.MEMBERSHIP_LISTENER;
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
            if (endpoint.live()) {
                final MemberImpl member = (MemberImpl) membershipEvent.getMember();
                endpoint.sendEvent(new ClientMembershipEvent(member, MembershipEvent.MEMBER_ADDED), getCallId());
            }
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (endpoint.live()) {
                final MemberImpl member = (MemberImpl) membershipEvent.getMember();
                endpoint.sendEvent(new ClientMembershipEvent(member, MembershipEvent.MEMBER_REMOVED), getCallId());
            }
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            if (endpoint.live()) {
                final MemberImpl member = (MemberImpl) memberAttributeEvent.getMember();
                final String uuid = member.getUuid();
                final MemberAttributeOperationType op = memberAttributeEvent.getOperationType();
                final String key = memberAttributeEvent.getKey();
                final Object value = memberAttributeEvent.getValue();
                final MemberAttributeChange memberAttributeChange = new MemberAttributeChange(uuid, op, key, value);
                endpoint.sendEvent(new ClientMembershipEvent(member, memberAttributeChange), getCallId());
            }
        }
    }
}
