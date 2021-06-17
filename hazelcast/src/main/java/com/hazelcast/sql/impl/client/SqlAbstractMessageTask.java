/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractAsyncMessageTask;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.sql.impl.operation.initiator.SqlQueryOperation;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for SQL messages.
 */
public abstract class SqlAbstractMessageTask<T, P> extends AbstractAsyncMessageTask<T, P> {
    protected SqlAbstractMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<P> processInternal() {
        SqlQueryOperation op = prepareOperation();
        UUID targetId = op.getQueryId().getMemberId();
        Address targetAddress = nodeEngine.getClusterService().getMember(targetId).getAddress();
        return nodeEngine.getOperationService()
                .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, targetAddress)
                .invoke();
    }

    protected abstract SqlQueryOperation prepareOperation();
}
