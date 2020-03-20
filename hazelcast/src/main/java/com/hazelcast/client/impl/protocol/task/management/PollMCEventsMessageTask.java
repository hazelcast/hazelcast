/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MCPollMCEventsCodec;
import com.hazelcast.client.impl.protocol.codec.MCPollMCEventsCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.MCEventDTO;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PollMCEventsMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    public PollMCEventsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ManagementCenterService mcs = nodeEngine.getManagementCenterService();
        if (mcs == null) {
            return Collections.<MCEventDTO>emptyList();
        }
        List<Event> polledEvents = mcs.pollMCEvents();
        List<MCEventDTO> result = new ArrayList<>(polledEvents.size());
        for (Event event : polledEvents) {
            result.add(MCEventDTO.fromEvent(event));
        }
        return result;
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCPollMCEventsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCPollMCEventsCodec.encodeResponse((List<MCEventDTO>) response);
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "pollMCEvents";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
