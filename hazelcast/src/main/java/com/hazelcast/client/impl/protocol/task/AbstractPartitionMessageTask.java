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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import java.util.concurrent.CompletableFuture;

/**
 * AbstractPartitionMessageTask
 */
public abstract class AbstractPartitionMessageTask<P>
        extends AbstractAsyncMessageTask<P, Object>
        implements PartitionSpecificRunnable {

    protected AbstractPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public int getPartitionId() {
        return clientMessage.getPartitionId();
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
//        NormalResponse response = new NormalResponse(null, clientMessage.getCorrelationId(), 0, false);
//        return CompletableFuture.completedFuture(response);

        Operation op = prepareOperation();
        if (ClientMessage.isFlagSet(clientMessage.getHeaderFlags(), ClientMessage.BACKUP_AWARE_FLAG)) {
            op.setClientCallId(clientMessage.getCorrelationId());
        }
        op.setCallerUuid(endpoint.getUuid());
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), op, getPartitionId())
                         .setResultDeserialized(false).invoke();

    }

    protected abstract Operation prepareOperation();

}
