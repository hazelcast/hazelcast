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

package com.hazelcast.client.impl.protocol.task.countdownlatch;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.CountDownLatchAwaitParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.countdownlatch.operations.AwaitOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class CountDownLatchAwaitMessageTask extends AbstractPartitionMessageTask<CountDownLatchAwaitParameters> {

    public CountDownLatchAwaitMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new AwaitOperation(parameters.name, parameters.timeout);
    }

    @Override
    protected CountDownLatchAwaitParameters decodeClientMessage(ClientMessage clientMessage) {
        return CountDownLatchAwaitParameters.decode(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return BooleanResultParameters.encode((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CountDownLatchPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "await";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.timeout, TimeUnit.MILLISECONDS};
    }
}
