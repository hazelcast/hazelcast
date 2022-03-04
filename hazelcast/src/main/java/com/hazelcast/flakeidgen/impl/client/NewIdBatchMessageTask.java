/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import com.hazelcast.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.IdBatchAndWaitTime;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.flakeidgen.impl.IdBatch;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.FlakeIdGeneratorPermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class NewIdBatchMessageTask
        extends AbstractMessageTask<RequestParameters> implements BlockingMessageTask {

    public NewIdBatchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected FlakeIdGeneratorNewIdBatchCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return FlakeIdGeneratorNewIdBatchCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        IdBatch idBatch = (IdBatch) response;
        return FlakeIdGeneratorNewIdBatchCodec.encodeResponse(idBatch.base(), idBatch.increment(), idBatch.batchSize());
    }

    @Override
    protected void processMessage() {
        UUID source = endpoint.getUuid();
        FlakeIdGeneratorProxy proxy = (FlakeIdGeneratorProxy) nodeEngine.getProxyService()
                                                                        .getDistributedObject(getServiceName(), parameters.name,
                                                                                source);
        final IdBatchAndWaitTime result = proxy.newIdBatch(parameters.batchSize);
        if (result.waitTimeMillis == 0) {
            sendResponse(result.idBatch);
        } else {
            nodeEngine.getExecutionService().schedule(new Runnable() {
                @Override
                public void run() {
                    sendResponse(result.idBatch);
                }
            }, result.waitTimeMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public String getServiceName() {
        return FlakeIdGeneratorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new FlakeIdGeneratorPermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "newIdBatch";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
