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
import com.hazelcast.client.impl.MessageListenerManager;
import com.hazelcast.client.proxy.ProxyHelper;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.protocol.Command;

import static com.hazelcast.client.proxy.ProxyHelper.check;

public class TopicClientProxy<T> implements ITopic {
    private final String name;
    private final ProxyHelper proxyHelper;

    private final Object lock = new Object();

    public TopicClientProxy(HazelcastClient client, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(client.getSerializationService(), client.getConnectionPool());
    }

    public String getName() {
        return name;
    }

    public void publish(Object message) {
        check(message);
        proxyHelper.doFireNForget(Command.TPUBLISH, new String[]{getName(), "noreply"}, proxyHelper.toData(message));
    }

    public void addMessageListener(MessageListener messageListener) {
        check(messageListener);
        synchronized (lock) {
            boolean shouldCall = messageListenerManager().noListenerRegistered(getName());
            messageListenerManager().registerListener(getName(), messageListener);
            if (shouldCall) {
                proxyHelper.doCommand(null, Command.TADDLISTENER, getName(), null);
            }
        }
    }

    public void removeMessageListener(MessageListener messageListener) {
        check(messageListener);
        synchronized (lock) {
            messageListenerManager().removeListener(getName(), messageListener);
            if (messageListenerManager().noListenerRegistered(getName())) {
                proxyHelper.doCommand(null, Command.TREMOVELISTENER, getName(), null);
            }
        }
    }

    private MessageListenerManager messageListenerManager() {
        return null;//proxyHelper.client.getListenerManager().getMessageListenerManager();
    }

    public void destroy() {
        proxyHelper.doCommand(null, Command.DESTROY, new String[]{"topic", getName()}, null);
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
