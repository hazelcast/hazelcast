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

package com.hazelcast.client;

import com.hazelcast.client.impl.MessageListenerManager;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ClusterOperation;

import static com.hazelcast.client.ProxyHelper.check;

public class TopicClientProxy<T> implements ITopic {
    private final String name;
    private final ProxyHelper proxyHelper;

    private final Object lock = new Object();

    public TopicClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public String getName() {
        return name.substring(Prefix.TOPIC.length());  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void publish(Object message) {
        check(message);
        proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_PUBLISH, message, null);
    }

    public void addMessageListener(MessageListener messageListener) {
        check(messageListener);
        synchronized (lock) {
            boolean shouldCall = messageListenerManager().noListenerRegistered(name);
            messageListenerManager().registerListener(name, messageListener);
            if (shouldCall) {
                doAddListenerCall(messageListener);
            }
        }
    }

    private void doAddListenerCall(MessageListener messageListener) {
        Call c = messageListenerManager().createNewAddListenerCall(proxyHelper);
        proxyHelper.doCall(c);
    }

    public void removeMessageListener(MessageListener messageListener) {
        check(messageListener);
        synchronized (lock) {
            messageListenerManager().removeListener(name, messageListener);
            if (messageListenerManager().noListenerRegistered(name)) {
                proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, null, null);
            }
        }
    }

    private MessageListenerManager messageListenerManager() {
        return proxyHelper.getHazelcastClient().getListenerManager().getMessageListenerManager();
    }

    public InstanceType getInstanceType() {
        return InstanceType.TOPIC;
    }

    public void destroy() {
        proxyHelper.destroy();
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
}
