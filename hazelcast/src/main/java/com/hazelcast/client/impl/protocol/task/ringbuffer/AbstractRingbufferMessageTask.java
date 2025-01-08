/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.ringbuffer;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.ringbuffer.impl.RingbufferService;

public abstract class AbstractRingbufferMessageTask<T> extends AbstractPartitionMessageTask<T> {

    public AbstractRingbufferMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }
    @Override
    public String getServiceName() {
        return RingbufferService.SERVICE_NAME;
    }

    @Override
    protected String getUserCodeNamespace() {
        RingbufferService service = nodeEngine.getService(getServiceName());
        RingbufferConfig config = service.getRingbufferConfig(getDistributedObjectName());
        return config == null ? null : config.getUserCodeNamespace();
    }
}
