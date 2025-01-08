/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.kafka.connect.impl.message.TaskConfigMessage;

import java.util.Properties;

// Test ConnectorWrapper that passes topic directly to task runners
public class TestSourceConnectorWrapper extends SourceConnectorWrapper {
    public TestSourceConnectorWrapper(Properties propertiesFromUser, HazelcastInstance instance) {
        // int processorOrder is 0 to make it master processor
        super(propertiesFromUser, 0, new TestProcessorContext()
                .setHazelcastInstance(instance));
    }

    @Override
    protected void publishMessage(TaskConfigMessage taskConfigMessage) {
        // Instead of publishing the message, pass it directly to processing function
        processMessage(taskConfigMessage);
    }

    @Override
    void destroyTopic() {
    }
}
