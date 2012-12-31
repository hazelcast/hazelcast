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

package com.hazelcast.topic;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.spi.*;
import com.hazelcast.topic.proxy.TopicProxy;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 1:50 PM
 */
public class TopicService implements ManagedService, RemoteService, EventPublishingService {

    public static final String NAME = "hz:impl:topicService";

    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, TopicProxy> proxies = new ConcurrentHashMap<String, TopicProxy>();

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {

    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            return new TopicProxy(name, nodeEngine);
        }
        TopicProxy proxy = proxies.get(name);
        if (proxy == null) {
            proxy = new TopicProxy(name, nodeEngine);
            final TopicProxy currentProxy = proxies.putIfAbsent(name, proxy);
            proxy = currentProxy != null ? currentProxy : proxy;
        }
        return proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }

    public void dispatchEvent(Object event, Object listener) {
        TopicEvent topicEvent = (TopicEvent) event;
        Message message = new Message(topicEvent.name, nodeEngine.toObject(topicEvent.data));
        ((MessageListener) listener).onMessage(message);

    }
}
