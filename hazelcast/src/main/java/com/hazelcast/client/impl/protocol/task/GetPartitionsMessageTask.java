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

import com.hazelcast.client.impl.ClientPartitionListenerService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.List;
import java.util.Map;

public class GetPartitionsMessageTask
        extends AbstractCallableMessageTask<ClientGetPartitionsCodec.RequestParameters> implements UrgentMessageTask {

    public GetPartitionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public int getPartitionId() {
        return -1;
    }

    /**
     * The partitions can be empty on the response
     * see {@link ClientPartitionListenerService#getPartitions(PartitionTableView)}
     */
    protected Object call() {
        InternalPartitionService service = getService(InternalPartitionService.SERVICE_NAME);
        service.firstArrangement();
        PartitionTableView partitionTableView = service.createPartitionTableView();
        int partitionStateVersion = partitionTableView.getVersion();

        Map<Address, List<Integer>> partitions = clientEngine.getPartitionListenerService().getPartitions(partitionTableView);
        return ClientGetPartitionsCodec.encodeResponse(partitions.entrySet(), partitionStateVersion);
    }

    @Override
    protected ClientGetPartitionsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientGetPartitionsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return (ClientMessage) response;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
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

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
