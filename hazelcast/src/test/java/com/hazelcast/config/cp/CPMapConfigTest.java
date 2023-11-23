/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.cp;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CPMapConfigTest {
    @Test
    public void testCopyConstructor() {
        CPMapConfig expected = new CPMapConfig("map", 15);
        CPMapConfig actual = new CPMapConfig(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testNameCtor() {
        CPMapConfig expected = new CPMapConfig("map");
        assertEquals("map", expected.getName());
        assertEquals(CPMapConfig.DEFAULT_MAX_SIZE_MB, expected.getMaxSizeMb());
    }


    @Test
    public void testSetters() {
        CPMapConfig config = new CPMapConfig();
        config.setName("map").setMaxSizeMb(1);

        assertEquals("map", config.getName());
        assertEquals(1, config.getMaxSizeMb());
    }

    @Test
    public void testValidation() {
        CPMapConfig config = new CPMapConfig();
        Throwable t = assertThrows(NullPointerException.class, () -> config.setName(null));
        assertEquals("Name must not be null", t.getMessage());

        t = assertThrows(IllegalArgumentException.class, () -> config.setMaxSizeMb(0));
        assertEquals("maxSizeMb is 0 but must be > 0", t.getMessage());

        t = assertThrows(IllegalArgumentException.class, () -> config.setMaxSizeMb(2001));
        assertEquals("maxSizeMb is 2001 but must be <= 2000", t.getMessage());
    }
}
