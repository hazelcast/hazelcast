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

package com.hazelcast.client.impl.protocol.task.executorservice.durable;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

import static com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;

public class DurableExecutorShutdownMessageTask
        extends AbstractCallableMessageTask<String> {

    public DurableExecutorShutdownMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        DistributedDurableExecutorService service = getService(SERVICE_NAME);
        service.shutdownExecutor(parameters);
        return null;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return DurableExecutorShutdownCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DurableExecutorShutdownCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return "shutdown";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
