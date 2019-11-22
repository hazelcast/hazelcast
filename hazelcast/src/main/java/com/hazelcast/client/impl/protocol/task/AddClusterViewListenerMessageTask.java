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

import com.hazelcast.client.impl.ClientClusterListenerService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.UuidUtil;

import java.security.Permission;
import java.util.UUID;

public class AddClusterViewListenerMessageTask
        extends AbstractCallableMessageTask<ClientAddClusterViewListenerCodec.RequestParameters>
        implements MembershipListener {

    private boolean advancedNetworkConfigEnabled;

    public AddClusterViewListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        advancedNetworkConfigEnabled = isAdvancedNetworkEnabled();
    }

    @Override
    protected Object call() {
        ClientClusterListenerService service = clientEngine.getClientClusterListenerService();
        service.registerListener(endpoint, clientMessage.getCorrelationId());
        endpoint.addDestroyAction(UuidUtil.newUnsecureUUID(), () -> {
            service.deregisterListener(endpoint);
            return Boolean.TRUE;
        });

        //second listener is for only attributes
        String serviceName = ClusterServiceImpl.SERVICE_NAME;
        ClusterServiceImpl clusterService = getService(serviceName);

        UUID registrationId = clusterService.addMembershipListener(this);
        endpoint.addListenerDestroyAction(serviceName, serviceName, registrationId);

        InternalPartitionService internalPartitionService = getService(InternalPartitionService.SERVICE_NAME);
        internalPartitionService.firstArrangement();
        return true;
    }

    @Override
    protected ClientAddClusterViewListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddClusterViewListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddClusterViewListenerCodec.encodeResponse();
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

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        //noop. Member view send insted of events via  ClientClusterListenerService
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        //noop. Member view send insted of events via  ClientClusterListenerService
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        if (!endpoint.isAlive()) {
            return;
        }

        MemberAttributeOperationType op = memberAttributeEvent.getOperationType();
        String key = memberAttributeEvent.getKey();
        String value = memberAttributeEvent.getValue() == null ? null : memberAttributeEvent.getValue().toString();
        MemberImpl member = (MemberImpl) memberAttributeEvent.getMember();
        ClientMessage eventMessage = ClientAddClusterViewListenerCodec
                .encodeMemberAttributeChangeEvent(translateMemberAddress(member), key, op.getId(), value);
        sendClientMessage(eventMessage);
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
