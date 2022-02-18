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

import com.hazelcast.internal.config.ScheduledExecutorConfigReadOnly;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledExecutorConfigTest extends HazelcastTestSupport {

    private ScheduledExecutorConfig config = new ScheduledExecutorConfig();

    @Test
    public void testConstructor_withName() {
        config = new ScheduledExecutorConfig("myName");

        assertEquals("myName", config.getName());
    }

    @Test
    public void testName() {
        config.setName("myName");

        assertEquals("myName", config.getName());
    }

    @Test
    public void testPoolSize() {
        config.setPoolSize(23);

        assertEquals(23, config.getPoolSize());
    }

    @Test
    public void testDurability() {
        config.setDurability(42);

        assertEquals(42, config.getDurability());
    }

    @Test
    public void testToString() {
        assertContains(config.toString(), "ScheduledExecutorConfig");
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(ScheduledExecutorConfig.class)
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .withPrefabValues(ScheduledExecutorConfigReadOnly.class,
                              new ScheduledExecutorConfigReadOnly(new ScheduledExecutorConfig("red")),
                              new ScheduledExecutorConfigReadOnly(new ScheduledExecutorConfig("black")))
                      .withPrefabValues(MergePolicyConfig.class,
                              new MergePolicyConfig(),
                              new MergePolicyConfig(DiscardMergePolicy.class.getSimpleName(), 10))
                      .verify();
    }
}
