/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.Collections.enumeration;
import static java.util.Collections.list;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Tests the {@link OverridePropertyRule} with multiple instances and the {@link PowerMockRunner}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OverridePropertyRulePowerMockTest.TestClass.class)
@Category({QuickTest.class, ParallelTest.class})
public class OverridePropertyRulePowerMockTest {

    @Rule
    public OverridePropertyRule overridePropertyRule = set("hazelcast.custom.system.property", "5");
    @Rule
    public OverridePropertyRule overridePreferIpv4Rule = set("java.net.preferIPv4Stack", "true");

    private NetworkInterface networkInterface = mock(NetworkInterface.class);

    @Before
    public void setUp() {
        mockStatic(NetworkInterface.class);
    }

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

    @Test
    public void testCustomPropertyWithPowerMock() throws Exception {
        TestClass testClass = createTestClass();

        assertEquals("5", testClass.getProperty("hazelcast.custom.system.property"));
    }

    @Test
    public void testHazelcastPropertyWithPowerMock() throws Exception {
        TestClass testClass = createTestClass();

        assertEquals("true", testClass.getProperty("java.net.preferIPv4Stack"));
    }

    private TestClass createTestClass() throws Exception {
        NetworkInterface networkInterface = mock(NetworkInterface.class);
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(networkInterface);
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(enumeration(networkInterfaces));

        return new TestClass();
    }

    public class TestClass {

        String getProperty(String property) throws Exception {
            // assert that PowerMock is working
            ArrayList<NetworkInterface> networkInterfaces = list(NetworkInterface.getNetworkInterfaces());
            assertEquals(1, networkInterfaces.size());
            assertEquals(networkInterface, networkInterfaces.get(0));

            return System.getProperty(property);
        }
    }
}
