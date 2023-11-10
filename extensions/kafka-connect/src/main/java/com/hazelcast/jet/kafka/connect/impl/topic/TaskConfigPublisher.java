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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TaskConfigPublisher {
    private final HazelcastInstance hazelcastInstance;
    private String topicName;
    // The reliableTopic is accessed by multiple threads
    private volatile ITopic<TaskConfigTopic> reliableTopic;
    private final List<UUID> listeners = new ArrayList<>();

    public TaskConfigPublisher(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public void createTopic(long executionId) {
        topicName = JobRepository.INTERNAL_JET_OBJECTS_PREFIX + executionId;
        reliableTopic = hazelcastInstance.getReliableTopic(topicName);
    }

    public void addListener(MessageListener<TaskConfigTopic> messageListener) {
        // Wrap the listener with LateJoiningListener to get only the latest message
        UUID uuid = reliableTopic.addMessageListener(new LateJoiningListener<>(
                hazelcastInstance,
                topicName,
                messageListener));
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
