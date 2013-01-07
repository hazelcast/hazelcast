/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.impl.MessageListenerManager;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.protocol.Command;

import static com.hazelcast.client.PacketProxyHelper.check;

public class TopicClientProxy<T> implements ITopic {
    private final String name;
    private final ProtocolProxyHelper protocolProxyHelper;

    private final Object lock = new Object();

    public TopicClientProxy(HazelcastClient client, String name) {
        this.name = name;
        protocolProxyHelper = new ProtocolProxyHelper(name, client);
    }

    public String getName() {
        return name.substring(Prefix.TOPIC.length());
    }

    public void publish(Object message) {
        check(message);
        protocolProxyHelper.doFireNForget(Command.TPUBLISH, new String[]{getName(), "noreply"}, protocolProxyHelper.toData(message));
    }

    public void addMessageListener(MessageListener messageListener) {
        check(messageListener);
        synchronized (lock) {
            boolean shouldCall = messageListenerManager().noListenerRegistered(getName());
            messageListenerManager().registerListener(getName(), messageListener);
            if (shouldCall) {
                protocolProxyHelper.doCommand(Command.TADDLISTENER, getName(), null);
            }
        }
    }

    public void removeMessageListener(MessageListener messageListener) {
        check(messageListener);
        synchronized (lock) {
            messageListenerManager().removeListener(getName(), messageListener);
            if (messageListenerManager().noListenerRegistered(getName())) {
                protocolProxyHelper.doCommand(Command.TREMOVELISTENER, getName(), null);
            }
        }
    }

    private MessageListenerManager messageListenerManager() {
        return protocolProxyHelper.client.getListenerManager().getMessageListenerManager();
    }

    public InstanceType getInstanceType() {
        return InstanceType.TOPIC;
    }

    public void destroy() {
        protocolProxyHelper.doCommand(Command.DESTROY, new String[]{InstanceType.TOPIC.name(), getName()}, null);
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
