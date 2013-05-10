/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.deprecated.topic.client;

import com.hazelcast.deprecated.client.ClientCommandHandler;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.Node;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.topic.TopicService;
import com.hazelcast.topic.proxy.TopicProxy;

public class TopicListenHandler extends ClientCommandHandler {
    final TopicService topic;

    public TopicListenHandler(TopicService topicService) {
        super();
        this.topic = topicService;
    }

    @Override
    public Protocol processCall(final Node node, Protocol protocol) {
        String name = protocol.args[0];
        final TopicProxy t = topic.createDistributedObjectForClient(name);
        final TcpIpConnection connection = protocol.conn;
        t.addMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                System.out.println(this + ", Received a message : " + message.getMessageObject());
                if (connection.live()) {
                    Object obj = message.getMessageObject();
                    Protocol response = new Protocol(connection, Command.EVENT, new String[]{}, node.serializationService.toData(obj));
                    sendResponse(node, response, connection);
                }else 
                    t.removeMessageListener(this);
            }
        });
        return null;
    }
}
