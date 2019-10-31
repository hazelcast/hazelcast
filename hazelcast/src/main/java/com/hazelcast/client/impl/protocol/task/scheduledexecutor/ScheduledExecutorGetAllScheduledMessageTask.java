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

package com.hazelcast.client.impl.protocol.task.scheduledexecutor;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.InvokeOnMembers;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerAccessor;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnMemberOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnPartitionOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.partition.IPartitionService;

import java.security.Permission;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class ScheduledExecutorGetAllScheduledMessageTask
        extends AbstractMessageTask<ScheduledExecutorGetAllScheduledFuturesCodec.RequestParameters>
        implements BlockingMessageTask {

    private final boolean advancedNetworkEnabled;

    public ScheduledExecutorGetAllScheduledMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        this.advancedNetworkEnabled = isAdvancedNetworkEnabled();
    }

    @Override
    protected void processMessage()
            throws Throwable {
        Map<Member, List<ScheduledTaskHandler>> scheduledTasks = new LinkedHashMap<Member, List<ScheduledTaskHandler>>();
        retrieveAllMemberOwnedScheduled(scheduledTasks);
        retrieveAllPartitionOwnedScheduled(scheduledTasks);
        sendResponse(scheduledTasks.entrySet());
    }

    @Override
    protected ScheduledExecutorGetAllScheduledFuturesCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorGetAllScheduledFuturesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorGetAllScheduledFuturesCodec
                .encodeResponse((Collection<Map.Entry<Member, List<ScheduledTaskHandler>>>) response);
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.schedulerName, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.schedulerName;
    }

    @Override
    public String getMethodName() {
        return "getAllScheduled";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.schedulerName};
    }

    private void retrieveAllMemberOwnedScheduled(Map<Member, List<ScheduledTaskHandler>> accumulator) {
        try {
            InvokeOnMembers invokeOnMembers = new InvokeOnMembers(nodeEngine, getServiceName(),
                    new GetAllScheduledOnMemberOperationFactory(parameters.schedulerName),
                    nodeEngine.getClusterService().getMembers());
            accumulateTaskHandlersAsUrnValues(accumulator, invokeOnMembers.invoke());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void retrieveAllPartitionOwnedScheduled(Map<Member, List<ScheduledTaskHandler>> accumulator) {
        try {
            accumulateTaskHandlersAsUrnValues(accumulator, nodeEngine.getOperationService().invokeOnAllPartitions(
                    getServiceName(), new GetAllScheduledOnPartitionOperationFactory(parameters.schedulerName)));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    private void accumulateTaskHandlersAsUrnValues(Map<Member, List<ScheduledTaskHandler>> accumulator,
                                                   Map<?, ?> taskHandlersMap) {

        ClusterService clusterService = nodeEngine.getClusterService();
        IPartitionService partitionService = nodeEngine.getPartitionService();

        for (Map.Entry<?, ?> entry : taskHandlersMap.entrySet()) {
            MemberImpl owner;
            Object key = entry.getKey();
            if (key instanceof Number) {
                owner = clusterService.getMember(partitionService.getPartitionOwner((Integer) key));
            } else {
                owner = (MemberImpl) key;
            }

            owner = translateMemberAddress(owner);

            List<ScheduledTaskHandler> handlers = (List<ScheduledTaskHandler>) entry.getValue();
            translateTaskHandlerAddresses(handlers);

            if (accumulator.containsKey(owner)) {
                List<ScheduledTaskHandler> memberUrns = accumulator.get(owner);
                memberUrns.addAll(handlers);
            } else {
                accumulator.put(owner, handlers);
            }
        }
    }

    private MemberImpl translateMemberAddress(MemberImpl member) {
        if (!advancedNetworkEnabled) {
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

    private void translateTaskHandlerAddresses(List<ScheduledTaskHandler> handlers) {
        if (!advancedNetworkEnabled) {
            return;
        }

        for (ScheduledTaskHandler handler : handlers) {
            if (handler.getAddress() != null) {
                ScheduledTaskHandlerAccessor.setAddress(handler,
                        clientEngine.clientAddressOf(handler.getAddress()));
            }
        }
    }

    private class GetAllScheduledOnMemberOperationFactory implements Supplier<Operation> {

        private final String schedulerName;

        GetAllScheduledOnMemberOperationFactory(String schedulerName) {
            this.schedulerName = schedulerName;
        }

        @Override
        public Operation get() {
            return new GetAllScheduledOnMemberOperation(schedulerName)
                    .setCallerUuid(endpoint.getUuid());
        }
    }
}
