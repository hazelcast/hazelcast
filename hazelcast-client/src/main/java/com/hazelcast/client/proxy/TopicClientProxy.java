/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.client.proxy.listener.MessageLRH;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.topic.TopicService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.client.proxy.ProxyHelper.check;

public class TopicClientProxy<T> implements ITopic {
    private final String name;
    private final ProxyHelper proxyHelper;
    Map<MessageListener<T>, ListenerThread> listenerMap = new ConcurrentHashMap<MessageListener<T>, ListenerThread>();
    private final Object lock = new Object();
    final HazelcastClient client;

    public TopicClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.client = client;
        proxyHelper = new ProxyHelper(client);
    }

    public String getName() {
        return name;
    }

    public void publish(Object message) {
        check(message);
        proxyHelper.doFireNForget(Command.TPUBLISH, new String[]{getName(), "noreply"}, proxyHelper.toData(message));
    }

    public void addMessageListener(MessageListener messageListener) {
        Protocol request = proxyHelper.createProtocol(Command.TLISTEN, new String[]{getName()}, null);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.topicListener.",
                client, request, new MessageLRH(messageListener, this));
        listenerMap.put(messageListener, thread);
        thread.start();
    }

    public void removeMessageListener(MessageListener messageListener) {
        System.out.println("ListenerMap sizeis " + listenerMap.size());
        ListenerThread thread = listenerMap.remove(messageListener);
        if (thread != null) {
            thread.stopListening();
        }
        System.out.println("ListenerMap sizeis " + listenerMap.size());
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{TopicService.SERVICE_NAME, getName()});
    }

    public Object getId() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ITopic) {
            return getName().equals(((ITopic) o).getName());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException();
    }
}
