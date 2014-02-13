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

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientPortableHook;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.cluster.AwsIpResolver;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.operation.MapOperationType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author mdogan 5/13/13
 */
public final class AddMembershipListenerRequest extends CallableClientRequest implements Portable, RetryableRequest {

    public AddMembershipListenerRequest() {
    }

    @Override
    public Object call() throws Exception {
        final ClusterServiceImpl service = getService();
        final ClientEndpoint endpoint = getEndpoint();
        final boolean awsEnabled = getClientEngine().getConfig().getNetworkConfig().getJoin().getAwsConfig().isEnabled();

        final String registrationId = service.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                if (endpoint.live()) {
                    MemberImpl member = (MemberImpl) membershipEvent.getMember();
                    if (awsEnabled)
                        member = resolveMember(member, false, service);
                    endpoint.sendEvent(new ClientMembershipEvent(member, MembershipEvent.MEMBER_ADDED), getCallId());
                }
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                if (endpoint.live()) {
                    MemberImpl member = (MemberImpl) membershipEvent.getMember();
                    if (awsEnabled)
                        member = resolveMember(member, false, service);
                    endpoint.sendEvent(new ClientMembershipEvent(member, MembershipEvent.MEMBER_REMOVED), getCallId());
                }
            }

            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
                if (endpoint.live()) {
                    MemberImpl member = (MemberImpl) memberAttributeEvent.getMember();
                    final String uuid = member.getUuid();
                    final MapOperationType op = memberAttributeEvent.getOperationType();
                    final String key = memberAttributeEvent.getKey();
                    final Object value = memberAttributeEvent.getValue();
                    final MemberAttributeChange memberAttributeChange = new MemberAttributeChange(uuid, op, key, value);
                    if (awsEnabled) {
                        member = resolveMember(member, false, service);
                    }
                    endpoint.sendEvent(new ClientMembershipEvent(member, memberAttributeChange), getCallId());
                }
            }
        });

        final String name = ClusterServiceImpl.SERVICE_NAME;
        endpoint.setListenerRegistration(name, name, registrationId);

        final Collection<MemberImpl> memberList = service.getMemberList();
        final Collection<Data> response = new ArrayList<Data>(memberList.size());
        final SerializationService serializationService = getClientEngine().getSerializationService();
        for (MemberImpl member : memberList) {
            MemberImpl resolvedMember = resolveMember(member, awsEnabled, service);
            response.add(serializationService.toData(resolvedMember));
        }
        return new SerializableCollection(response);
    }

    private MemberImpl resolveMember(MemberImpl member, boolean newMember, ClusterServiceImpl service) {
        if (newMember) {
            service.updateAwsIpResolver();
        }
        final AwsIpResolver awsIpResolver = service.getAwsIpResolver();
        final Address oldAddress = member.getAddress();
        String host = oldAddress.getHost();
        host = awsIpResolver.convertToPublic(host);
        final Address newAddress;
        try {
            newAddress = new Address(host, oldAddress.getPort());
            MemberImpl resolvedMember = new MemberImpl(member);
            resolvedMember.setAddress(newAddress);
            return resolvedMember;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return member;
        }

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

}
