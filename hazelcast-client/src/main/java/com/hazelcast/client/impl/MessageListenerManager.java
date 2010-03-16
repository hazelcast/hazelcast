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

package com.hazelcast.client.impl;

import com.hazelcast.client.Packet;
import com.hazelcast.core.MessageListener;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.client.Serializer.toObject;

public class MessageListenerManager {
    final private ConcurrentHashMap<String, List<MessageListener<Object>>> messageListeners = new ConcurrentHashMap<String, List<MessageListener<Object>>>();

    public void registerMessageListener(String name, MessageListener messageListener) {
        List<MessageListener<Object>> newListenersList = new CopyOnWriteArrayList<MessageListener<Object>>();
        List<MessageListener<Object>> listeners = messageListeners.putIfAbsent(name, newListenersList);
        if (listeners == null) {
            listeners = newListenersList;
        }
        listeners.add(messageListener);
    }

    public void removeMessageListener(String name, MessageListener messageListener) {
        if (!messageListeners.containsKey(name)) {
            return;
        }
        messageListeners.get(name).remove(messageListener);
        if (messageListeners.get(name).isEmpty()) {
            messageListeners.remove(name);
        }
    }

    public boolean noMessageListenerRegistered(String name) {
        if (!messageListeners.containsKey(name)) {
            return true;
        }
        return messageListeners.get(name).isEmpty();
    }

    public void notifyMessageListeners(Packet packet) {
        List<MessageListener<Object>> list = messageListeners.get(packet.getName());
        if (list != null) {
            for (MessageListener<Object> messageListener : list) {
                Object message = toObject(packet.getKey());
                messageListener.onMessage(message);
            }
        }
    }
}
