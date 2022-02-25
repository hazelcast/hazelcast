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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InterfacesTest extends HazelcastTestSupport {

    final String interfaceA = "127.0.0.1";
    final String interfaceB = "127.0.0.2";
    final String interfaceC = "127.0.0.3";

    @Test
    public void testIsEnabledByDefault() {
        InterfacesConfig interfaces = new InterfacesConfig();
        assertFalse(interfaces.isEnabled());
    }

    @Test
    public void testSetEnabled() {
        InterfacesConfig interfaces = new InterfacesConfig().setEnabled(true);
        assertTrue(interfaces.isEnabled());
    }

    @Test
    public void testAddInterface() {
        InterfacesConfig interfaces = new InterfacesConfig().addInterface(interfaceA);
        assertContains(interfaces.getInterfaces(), interfaceA);
    }

    @Test
    public void testClear() {
        InterfacesConfig interfaces = new InterfacesConfig()
                .addInterface(interfaceA)
                .addInterface(interfaceB)
                .addInterface(interfaceC);
        assertTrue(interfaces.getInterfaces().size() == 3);
        interfaces.clear();
        assertTrue(interfaces.getInterfaces().size() == 0);
    }

    @Test
    public void testGetInterfaceList() {
        InterfacesConfig interfaces = new InterfacesConfig();
        assertNotNull(interfaces.getInterfaces());
    }

    @Test
    public void testSetInterfaceList() {
        List<String> interfaceList = new ArrayList<String>();
        interfaceList.add(interfaceA);
        interfaceList.add(interfaceB);
        interfaceList.add(interfaceC);
        InterfacesConfig interfaces = new InterfacesConfig().setInterfaces(interfaceList);
        assertContains(interfaces.getInterfaces(), interfaceA);
        assertContains(interfaces.getInterfaces(), interfaceB);
        assertContains(interfaces.getInterfaces(), interfaceC);
    }

    @Test
    public void shouldNotContainDuplicateInterfaces() {
        InterfacesConfig interfaces = new InterfacesConfig().addInterface(interfaceA);
        assertTrue(interfaces.getInterfaces().size() == 1);
        interfaces.addInterface(interfaceA);
        assertTrue(interfaces.getInterfaces().size() == 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotBeModifiable() {
        new InterfacesConfig()
                .addInterface(interfaceA)
                .getInterfaces()
                .clear();
    }
}
