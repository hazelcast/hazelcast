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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ReactorResourceTest {

    @Test
    public void add_whenNull() {
        ReactorResources<String> resources = new ReactorResources<>(1024);
        assertThrows(NullPointerException.class, () -> resources.add(null));
    }

    @Test
    public void remove_whenNull() {
        ReactorResources<String> resources = new ReactorResources<>(1024);
        assertThrows(NullPointerException.class, () -> resources.remove(null));
    }

    @Test
    public void remove_whenNotAdded() {
        ReactorResources<String> resources = new ReactorResources<>(1024);
        resources.remove("foo");
    }

    @Test
    public void test_addAndRemove() {
        ReactorResources<String> resources = new ReactorResources<>(1024);

        resources.add("a1");
        resources.add("a2");

        resources.remove("a1");
        Set<String> found = new HashSet<>();
        resources.foreach(found::add);

        assertEquals(Collections.singleton("a2"), found);

        found.clear();
        resources.remove("a2");
        resources.foreach(found::add);
        assertEquals(Collections.emptySet(), found);
    }

    @Test
    public void test_add_whenRejected() {
        ReactorResources<String> resources = new ReactorResources<>(10);

        for (int k = 0; k < 10; k++) {
            assertTrue(resources.add("" + k));
        }

        assertFalse(resources.add("unwanted"));
        assertEquals(10, resources.size());
    }

    @Test
    public void test_forEach_whenNull() {
        ReactorResources<String> resources = new ReactorResources<>(1024);
        assertThrows(NullPointerException.class, () -> resources.foreach(null));
    }

    @Test
    public void test_forEach() {
        ReactorResources<String> resources = new ReactorResources<>(1024);

        Set<String> set = new HashSet<>();
        set.add("foo1");
        set.add("foo2");
        for (String s : set) {
            resources.add(s);
        }

        Set<String> found = new HashSet<>();
        resources.foreach(found::add);

        assertEquals(set, found);
    }
}
