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
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * AbstractAddressMessageTask
 */
public abstract class AbstractTargetMessageTask<P>
        extends AbstractAsyncMessageTask<P, Object> {

    protected AbstractTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        MemberImpl member = nodeEngine.getClusterService().getMember(getTargetUuid());
        if (member == null) {
            throw new TargetNotMemberException(String.format("Member with uuid(%s) is not in member list ", getTargetUuid()));
        }
        return nodeEngine.getOperationService()
                .createInvocationBuilder(getServiceName(), op, member.getAddress())
                .setResultDeserialized(false)
                .invoke();
    }

    protected abstract UUID getTargetUuid();

    protected abstract Operation prepareOperation();

}
