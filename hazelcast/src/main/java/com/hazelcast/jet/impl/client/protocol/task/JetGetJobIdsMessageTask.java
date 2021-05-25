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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class JetGetJobIdsMessageTask extends AbstractJetMessageTask<Void, Data> {

    JetGetJobIdsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                m -> null,
                JetGetJobIdsCodec::encodeResponse);
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        // TODO [viliam] send to all members and merge the results
        return completedFuture(serializationService.toData(GetJobIdsResult.EMPTY));
    }

    @Override
    // TODO [viliam] remove this method
    protected Operation prepareOperation() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getJobIds";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
