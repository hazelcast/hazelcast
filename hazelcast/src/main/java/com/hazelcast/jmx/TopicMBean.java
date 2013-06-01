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

package com.hazelcast.jmx;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * @ali 2/11/13
 */
@ManagedDescription("ITopic")
public class TopicMBean extends HazelcastMBean<ITopic> {

    private long totalMessageCount;
    private final String registrationId;

    protected TopicMBean(ITopic managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = createObjectName("Topic", managedObject.getName());
        MessageListener messageListener = new MessageListener() {
            public void onMessage(Message message) {
                totalMessageCount++;
            }
        };
        registrationId = managedObject.addMessageListener(messageListener);
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("totalMessageCount")
    public long getTotalMessageCount() {
        return totalMessageCount;
    }

    @ManagedAnnotation("config")
    public String getConfig() {
        return service.instance.getConfig().getTopicConfig(managedObject.getName()).toString();
    }


    public void preDeregister() throws Exception {
        super.preDeregister();
        managedObject.removeMessageListener(registrationId);
    }


}
