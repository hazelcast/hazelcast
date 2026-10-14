/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar;

import com.hazelcast.test.HazelcastTestSupport;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.util.List;
import java.util.concurrent.locks.Lock;

public final class PulsarUtils {

    private PulsarUtils() {
    }

    public static <T> Reader<T> newReader(String topicName, Schema<T> schema, List<T> results) {
        try {
            return PulsarTestSupport.getClient().newReader(schema)
                                    .topic(topicName)
                                    .subscriptionName(HazelcastTestSupport.randomName())
                                    .startMessageId(MessageId.earliest)
                                    .readerListener((r, msg) -> results.add(msg.getValue()))
                                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Reader<T> newReader(String topicName, Schema<T> schema, List<T> results, Lock lock) {
        try {
            lock.lock();
            return PulsarTestSupport.getClient().newReader(schema)
                                    .topic(topicName)
                                    .subscriptionName(HazelcastTestSupport.randomName())
                                    .startMessageId(MessageId.earliest)
                                    .readerListener((r, msg) -> results.add(msg.getValue()))
                                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }
}
