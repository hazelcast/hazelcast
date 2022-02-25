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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCResetQueueAgeStatisticsCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ManagementPermission;

import java.security.Permission;

public class QueueResetAgeStatisticsMessageTask extends AbstractCallableMessageTask<String> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("queue.resetAgeMetrics");

    public QueueResetAgeStatisticsMessageTask(ClientMessage clientMessage, Node node,
                                              Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call()
            throws Exception {
        QueueService service = getService(getServiceName());
        service.resetAgeStats(parameters);
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MCResetQueueAgeStatisticsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCResetQueueAgeStatisticsCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return "resetQueueAgeStatistics";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
