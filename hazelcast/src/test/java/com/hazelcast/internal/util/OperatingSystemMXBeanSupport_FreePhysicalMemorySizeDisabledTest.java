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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.OperatingSystemMXBeanSupport.COM_HAZELCAST_FREE_PHYSICAL_MEMORY_SIZE_DISABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("checkstyle:magicnumber")
public class OperatingSystemMXBeanSupport_FreePhysicalMemorySizeDisabledTest {

    private static final int DEFAULT_VALUE = -1;

    @After
    public void after() {
        System.clearProperty(COM_HAZELCAST_FREE_PHYSICAL_MEMORY_SIZE_DISABLED);
        OperatingSystemMXBeanSupport.reload();
    }

    @Test
    public void whenDisabled() {
        System.setProperty(COM_HAZELCAST_FREE_PHYSICAL_MEMORY_SIZE_DISABLED, "true");
        OperatingSystemMXBeanSupport.reload();
        long result = OperatingSystemMXBeanSupport.readLongAttribute("FreePhysicalMemorySize", DEFAULT_VALUE);
        assertEquals(DEFAULT_VALUE, result);
    }

    @Test
    public void whenDefault() {
        OperatingSystemMXBeanSupport.reload();
        long result = OperatingSystemMXBeanSupport.readLongAttribute("FreePhysicalMemorySize", DEFAULT_VALUE);
        assertNotEquals(DEFAULT_VALUE, result);
    }

    @Test
    public void whenEnabled() {
        System.setProperty(COM_HAZELCAST_FREE_PHYSICAL_MEMORY_SIZE_DISABLED, "false");
        OperatingSystemMXBeanSupport.reload();
        long result = OperatingSystemMXBeanSupport.readLongAttribute("FreePhysicalMemorySize", DEFAULT_VALUE);
        assertNotEquals(DEFAULT_VALUE, result);
    }
}
