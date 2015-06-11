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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

/**
 * Base callable Message task.
 */
public abstract class AbstractCallableMessageTask<P>
        extends AbstractMessageTask<P> {

    protected AbstractCallableMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public final void processMessage() {
        try {
            Object result = call();
            sendResponse(result);
        } catch (Exception e) {
            clientEngine.getLogger(getClass()).warning(e);
            sendClientMessage(e);
        }
    }

    protected abstract Object call() throws Exception;
}
