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

package com.hazelcast.client.proxy.listener;

import com.hazelcast.client.impl.DataMessage;
import com.hazelcast.client.proxy.TopicClientProxy;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

public class MessageLRH implements ListenerResponseHandler {
    final MessageListener messageListener;
    final TopicClientProxy topic;

    public MessageLRH(MessageListener messageListener, TopicClientProxy topic) {
        this.topic = topic;
        this.messageListener = messageListener;
    }

    public void handleResponse(Protocol response, SerializationService ss) throws Exception {
        Data object = response.buffers[0];
        Message message = new DataMessage(topic.getName(), object, ss);
        messageListener.onMessage(message);
    }

    public void onError(Exception e) {
        topic.addMessageListener(messageListener);
    }
}
