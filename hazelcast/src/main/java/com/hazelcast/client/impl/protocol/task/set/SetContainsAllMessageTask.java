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

package com.hazelcast.client.impl.protocol.task.set;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.SetContainsAllParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionContainsOperation;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.spi.Operation;
import java.security.Permission;

/**
 * SetContainsAllMessageTask
 */
public class SetContainsAllMessageTask
        extends AbstractPartitionMessageTask<SetContainsAllParameters> {

    public SetContainsAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionContainsOperation(parameters.name, parameters.valueSet);
    }

    @Override
    protected SetContainsAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return SetContainsAllParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.valueSet};
    }

    @Override
    public Permission getRequiredPermission() {
        return new SetPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "containsAll";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
