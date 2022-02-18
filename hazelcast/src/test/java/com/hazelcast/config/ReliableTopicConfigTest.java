/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.internal.config.ReliableTopicConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Executor;

import static com.hazelcast.config.ReliableTopicConfig.DEFAULT_READ_BATCH_SIZE;
import static com.hazelcast.config.ReliableTopicConfig.DEFAULT_STATISTICS_ENABLED;
import static com.hazelcast.config.ReliableTopicConfig.DEFAULT_TOPIC_OVERLOAD_POLICY;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static com.hazelcast.topic.TopicOverloadPolicy.DISCARD_NEWEST;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReliableTopicConfigTest {

    @Test
    public void testDefaultSettings() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");

        assertNull(config.getExecutor());
        assertEquals(DEFAULT_READ_BATCH_SIZE, config.getReadBatchSize());
        assertEquals("foo", config.getName());
        assertEquals(DEFAULT_TOPIC_OVERLOAD_POLICY, config.getTopicOverloadPolicy());
        assertEquals(DEFAULT_STATISTICS_ENABLED, config.isStatisticsEnabled());
    }

    @Test
    public void testCopyConstructorWithName() {
        ReliableTopicConfig original = new ReliableTopicConfig("original")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR)
                .setExecutor(mock(Executor.class))
                .setReadBatchSize(1)
                .setStatisticsEnabled(!DEFAULT_STATISTICS_ENABLED);

        ReliableTopicConfig copy = new ReliableTopicConfig(original, "copy");

        assertEquals("copy", copy.getName());
        assertSame(original.getExecutor(), copy.getExecutor());
        assertEquals(original.getReadBatchSize(), copy.getReadBatchSize());
        assertEquals(original.isStatisticsEnabled(), copy.isStatisticsEnabled());
        assertEquals(original.getTopicOverloadPolicy(), copy.getTopicOverloadPolicy());
    }

    @Test
    public void testCopyConstructor() {
        ReliableTopicConfig original = new ReliableTopicConfig("original")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR)
                .setExecutor(mock(Executor.class))
                .setReadBatchSize(1)
                .setStatisticsEnabled(!DEFAULT_STATISTICS_ENABLED);

        ReliableTopicConfig copy = new ReliableTopicConfig(original);

        assertEquals(original.getName(), copy.getName());
        assertSame(original.getExecutor(), copy.getExecutor());
        assertEquals(original.getReadBatchSize(), copy.getReadBatchSize());
        assertEquals(original.isStatisticsEnabled(), copy.isStatisticsEnabled());
        assertEquals(original.getTopicOverloadPolicy(), copy.getTopicOverloadPolicy());
    }

    // ==================== setTopicOverflowPolicy =============================

    @Test(expected = NullPointerException.class)
    public void setTopicOverloadPolicy_whenNull() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");
        config.setTopicOverloadPolicy(null);
    }

    @Test
    public void setTopicOverloadPolicy() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");
        config.setTopicOverloadPolicy(DISCARD_NEWEST);

        assertSame(DISCARD_NEWEST, config.getTopicOverloadPolicy());
    }

    // ==================== setReadBatchSize =============================\

    @Test
    public void setReadBatchSize() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");
        config.setReadBatchSize(200);

        assertEquals(200, config.getReadBatchSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setReadBatchSize_whenNegative() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");
        config.setReadBatchSize(-1);
    }

    // ==================== setStatisticsEnabled =============================\

    @Test
    public void setStatisticsEnabled() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");
        boolean newValue = !DEFAULT_STATISTICS_ENABLED;
        config.setStatisticsEnabled(newValue);

        assertEquals(newValue, config.isStatisticsEnabled());
    }

    // ===================== listener ===================


    @Test(expected = NullPointerException.class)
    public void addMessageListenerConfig_whenNull() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");

        config.addMessageListenerConfig(null);
    }

    @Test
    public void addMessageListenerConfig() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");

        ListenerConfig listenerConfig = new ListenerConfig("foobar");
        config.addMessageListenerConfig(listenerConfig);

        assertEquals(asList(listenerConfig), config.getMessageListenerConfigs());
    }

    // ==================== setExecutor =============================

    @Test
    public void setExecutor() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");

        Executor executor = mock(Executor.class);
        config.setExecutor(executor);

        assertSame(executor, config.getExecutor());

        config.setExecutor(null);

        assertNull(config.getExecutor());
    }

    @Test
    public void testReadonly() {
        Executor executor = mock(Executor.class);
        ReliableTopicConfig config = new ReliableTopicConfig("foo")
                .setReadBatchSize(201)
                .setExecutor(executor)
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR)
                .addMessageListenerConfig(new ListenerConfig("Foobar"));

        ReliableTopicConfig readOnly = new ReliableTopicConfigReadOnly(config);

        assertEquals(config.getName(), readOnly.getName());
        assertSame(config.getExecutor(), readOnly.getExecutor());
        assertEquals(config.isStatisticsEnabled(), readOnly.isStatisticsEnabled());
        assertEquals(config.getReadBatchSize(), readOnly.getReadBatchSize());
        assertEquals(config.getTopicOverloadPolicy(), readOnly.getTopicOverloadPolicy());
        assertEquals(config.getMessageListenerConfigs(), readOnly.getMessageListenerConfigs());

        try {
            readOnly.setExecutor(null);
            fail();
        } catch (UnsupportedOperationException e) {
        }

        try {
            readOnly.setReadBatchSize(3);
            fail();
        } catch (UnsupportedOperationException e) {
        }

        try {
            readOnly.setStatisticsEnabled(true);
            fail();
        } catch (UnsupportedOperationException e) {
        }

        try {
            readOnly.addMessageListenerConfig(new ListenerConfig("foobar"));
            fail();
        } catch (UnsupportedOperationException e) {
        }

        try {
            readOnly.setTopicOverloadPolicy(null);
            fail();
        } catch (UnsupportedOperationException e) {
        }
    }

    @Test
    public void test_toString() {
        ReliableTopicConfig config = new ReliableTopicConfig("foo");

        String s = config.toString();

        assertEquals("ReliableTopicConfig{name='foo', topicOverloadPolicy=BLOCK, executor=null,"
                + " readBatchSize=10, statisticsEnabled=true, listenerConfigs=[]}", s);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(ReliableTopicConfig.class)
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .verify();
    }
}
