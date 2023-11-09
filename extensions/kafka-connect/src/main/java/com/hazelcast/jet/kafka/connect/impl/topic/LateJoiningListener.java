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
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerAdapter;

/**
 * This listener will get the last published topic if it joins late.
 */
class LateJoiningListener<E> extends ReliableMessageListenerAdapter<E> {
    private final HazelcastInstance hazelcastInstance;
    private final String topicName;

    LateJoiningListener(HazelcastInstance hazelcastInstance, String topicName, MessageListener<E> messageListener) {
        super(messageListener);
        this.hazelcastInstance = hazelcastInstance;
        this.topicName = topicName;
    }

    @Override
    public long retrieveInitialSequence() {
        Ringbuffer<Object> ringbuffer = hazelcastInstance.getRingbuffer(RingbufferService.TOPIC_RB_PREFIX + topicName);
        // if tailSequence is -1 get next message
        // if tailSequence is different from -1 get the oldest message
        return ringbuffer.tailSequence();
    }
}
