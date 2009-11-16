/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.impl.ClusterOperation;

public class TopicClientProxy<T> implements ClientProxy, ITopic {
    private String name;
    private HazelcastClient client;
    private ProxyHelper proxyHelper;

    public TopicClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
		this.client = hazelcastClient;
		proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public void setOutRunnable(OutRunnable out) {
        proxyHelper.setOutRunnable(out);
    }

    public String getName() {
        return name.substring(2);  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void publish(Object message) {
        proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_PUBLISH, message, null);
    }

    public void addMessageListener(MessageListener messageListener) {
        if(!client.listenerManager.allreadyRegisteredAMessageListener(name)){
            proxyHelper.doOp(ClusterOperation.ADD_LISTENER, null, null);
        }
        client.listenerManager.registerMessageListener(name, messageListener);
    }

    public void removeMessageListener(MessageListener messageListener) {
        client.listenerManager.removeMessageListener(name, messageListener);
        if(!client.listenerManager.allreadyRegisteredAMessageListener(name)){
            proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, null, null);
        }
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
}
