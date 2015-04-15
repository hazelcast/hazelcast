/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.ListCompareAndRemoveAllParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.collection.operations.CollectionCompareAndRemoveOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.Operation;
import java.security.Permission;

/**
 * ListCompareAndRemoveAllMessageTask
 */
public class ListCompareAndRemoveAllMessageTask
        extends AbstractPartitionMessageTask<ListCompareAndRemoveAllParameters> {

    public ListCompareAndRemoveAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionCompareAndRemoveOperation(parameters.name, false, parameters.valueSet);
    }

    @Override
    protected ListCompareAndRemoveAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListCompareAndRemoveAllParameters.decode(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.valueSet};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "removeAll";
    }

}
