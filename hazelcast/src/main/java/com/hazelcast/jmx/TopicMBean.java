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

package com.hazelcast.jmx;

import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * Management bean for Hazelcst Topic
 *
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("A distributed queue")
public class TopicMBean extends AbstractMBean<ITopic<?>> {

    @SuppressWarnings("unchecked")
    protected MessageListener listener;

    private StatisticsCollector servedStats = null;

    public TopicMBean(ITopic<?> topic, ManagementService managementService) {
        super(topic, managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("Topic", getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void postRegister(Boolean registrationDone) {
        super.postRegister(registrationDone);
        if (!registrationDone) {
            return;
        }
        if (managementService.showDetails()) {
            servedStats = ManagementService.newStatisticsCollector();
            listener = new MessageListener() {

                public void onMessage(Message msg) {
                    servedStats.addEvent();
                }
            };
            getManagedObject().addMessageListener(listener);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void preDeregister() throws Exception {
        if (listener != null) {
            getManagedObject().removeMessageListener(listener);
            listener = null;
        }
        if (servedStats != null) {
            servedStats.destroy();
            servedStats = null;
        }
        super.preDeregister();
    }

    /**
     * Resets statistics
     */
    @JMXOperation("resetStats")
    public void resetStats() {
        if (servedStats != null)
            servedStats.reset();
    }

    @JMXAttribute("Name")
    @JMXDescription("Registration name of the queue")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXAttribute("Config")
    @JMXDescription("Topic configuration")
    public String getConfig() {
        final TopicConfig config = managementService.instance.getConfig().getTopicConfig(getName());
        return config.toString();
    }

    @JMXAttribute("MessagesDispatched")
    @JMXDescription("Total messages dispatched since creation")
    public long getItemsReceived() {
        return servedStats.getTotal();
    }

    @JMXAttribute("MessagesDispatchedLast")
    @JMXDescription("Messages dispatched in the last second")
    public double getItemsReceivedAvg() {
        return servedStats.getAverage();
    }

    @JMXAttribute("MessagesDispatchedPeak")
    @JMXDescription("Max messages dispatched  per second")
    public double getItemsReceivedMax() {
        return servedStats.getMax();
    }
}
