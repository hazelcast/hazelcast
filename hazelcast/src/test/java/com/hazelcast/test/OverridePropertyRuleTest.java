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

package com.hazelcast.test;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link OverridePropertyRule} with multiple instances and a Hazelcast specific runner.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OverridePropertyRuleTest {

    @Rule
    public OverridePropertyRule overridePropertyRule = set("hazelcast.custom.system.property", "5");
    @Rule
    public OverridePropertyRule overridePreferIpv4Rule = set("java.net.preferIPv4Stack", "true");

    @Test
    public void testNonExistingProperty() {
        assertNull(System.getProperty("notExists"));
    }

    @Test
    public void testCustomSystemProperty() {
        assertEquals("5", System.getProperty("hazelcast.custom.system.property"));
    }

    @Test
    public void testHazelcastProperty() {
        assertEquals("true", System.getProperty("java.net.preferIPv4Stack"));
    }

    @Test
    public void testHazelcastPropertyWithGetBoolean() {
        assertTrue(Boolean.getBoolean("java.net.preferIPv4Stack"));
    }
}
