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

import java.util.UUID;

/**
 * Base message task for listener registration tasks
 */
public abstract class AbstractAddListenerMessageTask<P>
        extends AbstractAsyncMessageTask<P, UUID> {

    protected AbstractAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        // We always want to be Namespace-aware when adding listeners
        NamespaceUtil.runWithNamespace(nodeEngine, getNamespace(), super::processMessage);
    }

    @Override
    protected Object processResponseBeforeSending(UUID response) {
        addDestroyAction(response);
        return response;
    }

    protected void addDestroyAction(UUID registrationId) {
        endpoint.addListenerDestroyAction(getServiceName(), getDistributedObjectName(), registrationId);
    }

    protected abstract String getNamespace();
}
