/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.NamespacePermission;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;

/**
 * AbstractPartitionMessageTask
 */
public abstract class AbstractPartitionMessageTask<P>
        extends AbstractAsyncMessageTask<P, Object>
        implements PartitionSpecificRunnable {

    private boolean namespaceAware;
    protected AbstractPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    /**
     * Used to mark the inheriting task as Namespace-aware
     */
    protected final void setNamespaceAware() {
        this.namespaceAware = true;
    }

    @Override
    protected void processMessage() {
        // Providing Namespace awareness here covers calls in #beforeProcess() as well as #processInternal()
        if (namespaceAware) {
            NamespaceUtil.runWithNamespace(nodeEngine, getNamespace(), super::processMessage);
        } else {
            super.processMessage();
        }
    }

    @Override
    public int getPartitionId() {
        return clientMessage.getPartitionId();
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        Operation op = prepareOperation();
        if (ClientMessage.isFlagSet(clientMessage.getHeaderFlags(), ClientMessage.BACKUP_AWARE_FLAG)) {
            op.setClientCallId(clientMessage.getCorrelationId());
        }
        op.setCallerUuid(endpoint.getUuid());
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), op, getPartitionId())
                         .setResultDeserialized(false).invoke();
    }

    @Override
    public final Permission getNamespacePermission() {
        if (namespaceAware) {
            String namespace = getNamespace();
            return namespace != null ? new NamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
        }
        return null;
    }

    protected abstract Operation prepareOperation();

    protected abstract String getNamespace();
}
