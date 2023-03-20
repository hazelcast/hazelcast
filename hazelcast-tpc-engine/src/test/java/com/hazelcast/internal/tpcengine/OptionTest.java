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

package com.hazelcast.internal.tpcengine;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class OptionTest {

    @Test
    public void test_name_type() {
        String name = "foo";
        Class type = Integer.class;
        Option option = new Option(name, type);
        assertEquals(name, option.name());
        assertEquals(type, option.type());
    }

    @Test
    public void test_equals() {
        Option self = new Option("foo", String.class);

        assertEquals(self, self);
        assertNotEquals(self, null);
        assertEquals(new Option("foo", String.class), new Option("foo", String.class));
        assertNotEquals(new Option("bar", String.class), new Option("foo", String.class));
        // option equality is based on name only
        assertEquals(new Option("foo", String.class), new Option("foo", Integer.class));
        assertNotEquals(new Option("bar", String.class), new Option("foo", Integer.class));
    }

    @Test
    public void test_hash() {
        assertEquals(new Option("foo", String.class).hashCode(), new Option("foo", String.class).hashCode());

        //hash is only based on name
        assertEquals(new Option("foo", String.class).hashCode(), new Option("foo", Integer.class).hashCode());
    }
}
