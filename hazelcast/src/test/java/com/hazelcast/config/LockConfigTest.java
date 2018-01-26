/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockConfigTest {

    private LockConfig config = new LockConfig();

    @Test
    public void testConstructor_withName() {
        config = new LockConfig("foobar");

        assertEquals("foobar", config.getName());
    }

    @Test
    public void testConstructor_withLockConfig() {
        config.setName("myName");
        config.setQuorumName("myQuorum");

        LockConfig cloned = new LockConfig(config);

        assertEquals(config.getName(), cloned.getName());
        assertEquals(config.getQuorumName(), cloned.getQuorumName());
    }

    @Test
    public void testConstructor_withLockConfigAndOverriddenName() {
        config.setName("myName");
        config.setQuorumName("myQuorum");

        LockConfig cloned = new LockConfig("newName", config);

        assertEquals("newName", cloned.getName());
        assertNotEquals(config.getName(), cloned.getName());
        assertEquals(config.getQuorumName(), cloned.getQuorumName());
    }

    @Test
    public void testToString() {
        assertNotNull(config.toString());
        assertContains(config.toString(), "LockConfig");
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(LockConfig.class)
                      .allFieldsShouldBeUsed()
                      .suppress(Warning.NONFINAL_FIELDS)
                      .verify();
    }
}
