/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect.impl.topic;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.ReliableMessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TaskConfigPublisher {
    private ITopic<TaskConfigTopic> reliableTopic;

    private List<UUID> listeners = new ArrayList<>();

    public void createTopic(HazelcastInstance hazelcastInstance, long executionId) {
        Config config = hazelcastInstance.getConfig();

        String topicName = String.valueOf(executionId);
        /*RingbufferConfig ringbufferConfig = config.getRingbufferConfig(topicName);
        ringbufferConfig.setCapacity(1);
        */
        reliableTopic = hazelcastInstance.getReliableTopic(topicName);
    }

    public void addListener(ReliableMessageListener<TaskConfigTopic> reliableMessageListener) {
        UUID uuid = reliableTopic.addMessageListener(reliableMessageListener);
        listeners.add(uuid);
    }

    public void publish(TaskConfigTopic taskConfigTopic) {
        reliableTopic.publish(taskConfigTopic);
    }

    public void destroyTopic() {
        reliableTopic.destroy();
    }

    public void removeListeners() {
        for (UUID listener : listeners) {
            reliableTopic.removeMessageListener(listener);
        }
    }
}
