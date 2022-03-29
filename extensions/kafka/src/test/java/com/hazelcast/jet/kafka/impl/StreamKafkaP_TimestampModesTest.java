/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStageTestBase;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static java.util.Arrays.asList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamKafkaP_TimestampModesTest extends StreamSourceStageTestBase {

    private static KafkaTestSupport kafkaTestSupport = KafkaTestSupport.create();

    private static Properties properties;

    private String topicName;

    @BeforeClass
    public static void beforeClass1() throws IOException {
        kafkaTestSupport.createKafkaCluster();
        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
    }

    @AfterClass
    public static void afterClass1() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Before
    public void before1() {
        topicName = randomString();
        kafkaTestSupport.createTopic(topicName, 1);
        kafkaTestSupport.produce(topicName, 0, 1L, 1, "1");
        kafkaTestSupport.produce(topicName, 0, 2L, 2, "2");
    }

    private StreamSource<Integer> getSource() {
        return KafkaSources.<Integer, Integer, Integer>kafka(properties, ConsumerRecord::key, topicName);
    }

    @Test
    public void test_sourceKafka_withoutTimestamps() {
        test(getSource(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceKafka_withNativeTimestamps() {
        test(getSource(), withNativeTimestampsFn, asList(1L, 2L), null);
    }

    @Test
    public void test_sourceKafka_withTimestamps() {
        test(getSource(), withTimestampsFn, asList(2L, 3L), null);
    }
}
