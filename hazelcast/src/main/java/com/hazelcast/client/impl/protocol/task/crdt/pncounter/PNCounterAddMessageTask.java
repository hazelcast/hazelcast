/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.crdt.pncounter;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.PNCounterAddCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterAddCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractAddressMessageTask;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.crdt.pncounter.PNCounterService;
import com.hazelcast.crdt.pncounter.operations.AddOperation;
import com.hazelcast.crdt.pncounter.operations.CRDTTimestampedLong;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.PNCounterPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.Map.Entry;

/**
 * Task responsible for processing client messages for updating the
 * {@link com.hazelcast.crdt.pncounter.PNCounter} state.
 * If this message was sent from a client with smart routing disabled, the
 * member may forward the request to a different target member.
 */
public class PNCounterAddMessageTask extends AbstractAddressMessageTask<RequestParameters> {

    public PNCounterAddMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Address getAddress() {
        return parameters.targetReplica;
    }

    @Override
    protected Operation prepareOperation() {
        final VectorClock vectorClock = new VectorClock();
        if (parameters.replicaTimestamps != null) {
            for (Entry<String, Long> timestamp : parameters.replicaTimestamps) {
                vectorClock.setReplicaTimestamp(timestamp.getKey(), timestamp.getValue());
            }
        }

        return new AddOperation(parameters.name, parameters.delta, parameters.getBeforeUpdate, vectorClock);
    }


    @Override
    protected PNCounterAddCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return PNCounterAddCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        final CRDTTimestampedLong resp = (CRDTTimestampedLong) response;
        final PNCounterConfig counterConfig = nodeEngine.getConfig().findPNCounterConfig(parameters.name);
        return PNCounterAddCodec.encodeResponse(
                resp.getValue(), resp.getVectorClock().entrySet(), counterConfig.getReplicaCount());
    }

    @Override
    public String getServiceName() {
        return PNCounterService.SERVICE_NAME;
    }

    public Object[] getParameters() {
        return new Object[]{parameters.delta, parameters.getBeforeUpdate};
    }

    @Override
    public Permission getRequiredPermission() {
        return new PNCounterPermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getMethodName() {
        return "get";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
