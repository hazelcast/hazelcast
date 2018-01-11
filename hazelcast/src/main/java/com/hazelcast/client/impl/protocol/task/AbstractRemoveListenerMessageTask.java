/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

/**
 * Base class for remove listener message tasks that removes a client listener registration
 * from a node
 *
 * @param <P> listener registration request parameters type
 */
public abstract class AbstractRemoveListenerMessageTask<P> extends AbstractCallableMessageTask<P> {

    protected AbstractRemoveListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    public final Object call() {
        endpoint.removeDestroyAction(getRegistrationId());
        return deRegisterListener();
    }

    protected abstract boolean deRegisterListener();

    protected abstract String getRegistrationId();

    @Override
    public Object[] getParameters() {
        return new Object[]{getRegistrationId()};
    }
}
