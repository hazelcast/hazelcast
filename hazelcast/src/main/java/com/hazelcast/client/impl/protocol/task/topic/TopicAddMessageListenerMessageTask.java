/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.TopicAddMessageListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.ListenerMessageTask;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.topic.impl.DataAwareMessage;
import com.hazelcast.topic.impl.TopicService;

import java.security.Permission;
import java.util.Random;
import java.util.UUID;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

public class TopicAddMessageListenerMessageTask
        extends AbstractCallableMessageTask<TopicAddMessageListenerCodec.RequestParameters>
        implements MessageListener, ListenerMessageTask {

    private Data partitionKey;
    private Random rand = new Random();

    public TopicAddMessageListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        partitionKey = serializationService.toData(parameters.name);
        TopicService service = getService(TopicService.SERVICE_NAME);
        UUID registrationId = service.addMessageListener(parameters.name, this, parameters.localOnly);
        endpoint.addListenerDestroyAction(TopicService.SERVICE_NAME, parameters.name, registrationId);
        return registrationId;
    }

    @Override
    protected TopicAddMessageListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TopicAddMessageListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TopicAddMessageListenerCodec.encodeResponse((UUID) response);
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
        return "addMessageListener";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null};
    }


    @Override
    public void onMessage(Message message) {
        if (!endpoint.isAlive()) {
            return;
        }

        if (!(message instanceof DataAwareMessage)) {
            throw new IllegalArgumentException("Expecting: DataAwareMessage, Found: "
                    + message.getClass().getSimpleName());
        }

        DataAwareMessage dataAwareMessage = (DataAwareMessage) message;
        Data messageData = dataAwareMessage.getMessageData();
        UUID publisherUuid = message.getPublishingMember().getUuid();
        ClientMessage eventMessage = TopicAddMessageListenerCodec.encodeTopicEvent(messageData,
                message.getPublishTime(), publisherUuid);

        boolean isMultithreaded = nodeEngine.getConfig().findTopicConfig(parameters.name).isMultiThreadingEnabled();
        if (isMultithreaded) {
            int key = rand.nextInt();
            int partitionId = hashToIndex(key, nodeEngine.getPartitionService().getPartitionCount());
            eventMessage.setPartitionId(partitionId);
            sendClientMessage(eventMessage);
        } else {
            sendClientMessage(partitionKey, eventMessage);
        }
    }
}
