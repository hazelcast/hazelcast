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

import com.hazelcast.internal.config.QueueConfigReadOnly;
import com.hazelcast.internal.config.QueueStoreConfigReadOnly;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueConfigTest {

    private QueueConfig queueConfig = new QueueConfig();

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#getName()}.
     */
    @Test
    public void testGetName() {
        assertNull(null, queueConfig.getName());
    }

    @Test
    public void testSetName() {
        String name = "a test name";
        queueConfig.setName(name);
        assertEquals(name, queueConfig.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenItsNegative() {
        queueConfig.setAsyncBackupCount(-1);
    }

    @Test
    public void setAsyncBackupCount_whenItsZero() {
        queueConfig.setAsyncBackupCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenTooLarge() {
        // max allowed is 6
        queueConfig.setAsyncBackupCount(200);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_whenItsNegative() {
        queueConfig.setBackupCount(-1);
    }

    @Test
    public void setBackupCount_whenItsZero() {
        queueConfig.setBackupCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_tooLarge() {
        // max allowed is 6
        queueConfig.setBackupCount(200);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(QueueConfig.class)
                .suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS)
                .withPrefabValues(QueueConfigReadOnly.class,
                        new QueueConfigReadOnly(new QueueConfig("red")),
                        new QueueConfigReadOnly(new QueueConfig("black")))
                .withPrefabValues(QueueStoreConfigReadOnly.class,
                        new QueueStoreConfigReadOnly(new QueueStoreConfig().setClassName("red")),
                        new QueueStoreConfigReadOnly(new QueueStoreConfig().setClassName("black")))
                .withPrefabValues(MergePolicyConfig.class,
                        new MergePolicyConfig(PutIfAbsentMergePolicy.class.getName(), 100),
                        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 200))
                .verify();
    }
}
