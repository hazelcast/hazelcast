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

package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.impl.monitor.LocalTopicStatsImpl;
import com.hazelcast.impl.monitor.TopicOperationsCounter;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.DataSerializable;

public class TopicProxyImpl extends FactoryAwareNamedProxy implements TopicProxy, DataSerializable {
    private transient TopicProxy base = null;
    private TopicManager topicManager = null;
    private ListenerManager listenerManager = null;

    public TopicProxyImpl() {
    }

    TopicProxyImpl(String name, FactoryImpl factory) {
        set(name, factory);
        base = new TopicProxyReal();
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (TopicProxy) factory.getOrCreateProxyByName(name);
        }
    }

    public void set(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        super.setHazelcastInstance(hazelcastInstance);
        topicManager = factory.node.topicManager;
        listenerManager = factory.node.listenerManager;
    }

    public String getLongName() {
        return base.getLongName();
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    @Override
    public String toString() {
        return "Topic [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicProxyImpl that = (TopicProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public void publish(Object msg) {
        ensure();
        base.publish(msg);
    }

    public void addMessageListener(MessageListener listener) {
        ensure();
        base.addMessageListener(listener);
    }

    public void removeMessageListener(MessageListener listener) {
        ensure();
        base.removeMessageListener(listener);
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public String getName() {
        ensure();
        return base.getName();
    }

    public LocalTopicStats getLocalTopicStats() {
        ensure();
        return base.getLocalTopicStats();
    }

    public TopicOperationsCounter getTopicOperationCounter() {
        return base.getTopicOperationCounter();
    }

    class TopicProxyReal implements TopicProxy {
        TopicOperationsCounter topicOperationsCounter = new TopicOperationsCounter();

        public void publish(Object msg) {
            Util.checkSerializable(msg);
            topicOperationsCounter.incrementPublishes();
            topicManager.doPublish(name, msg);
        }

        public void addMessageListener(MessageListener listener) {
            listenerManager.addListener(name, listener, null, true,
                    getInstanceType());
        }

        public void removeMessageListener(MessageListener listener) {
            listenerManager.removeListener(name, listener, null);
        }

        public void destroy() {
            factory.destroyInstanceClusterWide(name, null);
        }

        public InstanceType getInstanceType() {
            return InstanceType.TOPIC;
        }

        public String getName() {
            return name.substring(Prefix.TOPIC.length());
        }

        public String getLongName() {
            return name;
        }

        public Object getId() {
            return name;
        }

        public LocalTopicStats getLocalTopicStats() {
            LocalTopicStatsImpl localTopicStats = topicManager.getTopicInstance(name).getTopicStats();
            localTopicStats.setOperationStats(topicOperationsCounter.getPublishedStats());
            return localTopicStats;
        }

        public TopicOperationsCounter getTopicOperationCounter() {
            return topicOperationsCounter;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}
