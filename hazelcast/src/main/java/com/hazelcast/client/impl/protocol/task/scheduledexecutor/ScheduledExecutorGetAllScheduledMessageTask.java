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

package com.hazelcast.client.impl.protocol.task.scheduledexecutor;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.InvokeOnMembers;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnMemberOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class ScheduledExecutorGetAllScheduledMessageTask
        extends AbstractMessageTask<ScheduledExecutorGetAllScheduledFuturesCodec.RequestParameters> {

    private static final int GET_ALL_SCHEDULED_TIMEOUT = 10;

    public ScheduledExecutorGetAllScheduledMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage()
            throws Throwable {
        Map<Member, List<String>> scheduledTasks = new LinkedHashMap<Member, List<String>>();
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
                .encodeResponse((Collection<Map.Entry<Member, List<String>>>) response);
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
        return new Object[] { parameters.schedulerName };
    }

    private void retrieveAllMemberOwnedScheduled(Map<Member, List<String>> accumulator) {
        try {
            InvokeOnMembers invokeOnMembers = new InvokeOnMembers(nodeEngine, getServiceName(),
                    new GetAllScheduledOnMemberOperationFactory(parameters.schedulerName),
                    nodeEngine.getClusterService().getMembers());
            accumulateTaskHandlersAsUrnValues(accumulator, invokeOnMembers.invoke());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void retrieveAllPartitionOwnedScheduled(Map<Member, List<String>> accumulator) {
        try {
            accumulateTaskHandlersAsUrnValues(accumulator, nodeEngine.getOperationService().invokeOnAllPartitions(
                    getServiceName(), new GetAllScheduledOperationFactory(parameters.schedulerName)));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    private void accumulateTaskHandlersAsUrnValues(Map<Member, List<String>> accumulator,
                                                   Map<?, ?> taskHandlersMap) {

        ClusterService clusterService = nodeEngine.getClusterService();
        IPartitionService partitionService = nodeEngine.getPartitionService();

        for (Map.Entry<?, ?> entry : taskHandlersMap.entrySet()) {
            Member owner;
            Object key = entry.getKey();
            if (key instanceof Number) {
                owner = clusterService.getMember(partitionService.getPartitionOwner((Integer) key));
            } else {
                owner = (Member) key;
            }

            List<ScheduledTaskHandler> handlers = (List<ScheduledTaskHandler>) entry.getValue();
            List<String> urns = new ArrayList<String>();

            for (ScheduledTaskHandler handler : handlers) {
                urns.add(handler.toUrn());
            }

            if (accumulator.containsKey(owner)) {
                List<String> memberUrns = accumulator.get(owner);
                memberUrns.addAll(urns);
            } else {
                accumulator.put(owner, urns);
            }
        }
    }

    private class GetAllScheduledOnMemberOperationFactory implements OperationFactory {

        private final String schedulerName;

        GetAllScheduledOnMemberOperationFactory(String schedulerName) {
            this.schedulerName = schedulerName;
        }

        @Override
        public Operation createOperation() {
            return new GetAllScheduledOnMemberOperation(schedulerName)
                    .setCallerUuid(endpoint.getUuid());
        }

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
        }
    }
}
