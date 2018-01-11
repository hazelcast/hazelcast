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

package com.hazelcast.client.impl.protocol.task.topic;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractRemoveListenerMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.topic.impl.TopicService;

import java.security.Permission;

public class TopicRemoveMessageListenerMessageTask
        extends AbstractRemoveListenerMessageTask<TopicRemoveMessageListenerCodec.RequestParameters> {

    public TopicRemoveMessageListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean deRegisterListener() {
        TopicService service = getService(TopicService.SERVICE_NAME);
        return service.removeMessageListener(parameters.name, parameters.registrationId);
    }

    @Override
    protected String getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected TopicRemoveMessageListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TopicRemoveMessageListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TopicRemoveMessageListenerCodec.encodeResponse((Boolean) response);
    }


    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "removeMessageListener";
    }

}
