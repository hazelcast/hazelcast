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

package com.hazelcast.jet.stream;

import org.junit.Test;

import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ToArrayTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        Object[] objects = streamMap().toArray();

        assertArray(objects, Entry.class);
    }

    @Test
    public void sourceCache() {
        Object[] objects = streamCache().toArray();

        assertArray(objects, Entry.class);
    }

    private void assertArray(Object[] objects, Class clazz) {
        assertEquals(COUNT, objects.length);
        for (int i = 0; i < COUNT; i++) {
            assertInstanceOf(clazz, objects[i]);
            assertNotNull(objects[i]);
        }
    }

    @Test
    public void sourceMap_withArraySupplier() {
        Entry[] entries = streamMap().toArray(Entry[]::new);

        assertArray(entries, Entry.class);
    }

    @Test
    public void sourceCache_withArraySupplier() {
        Entry[] entries = streamCache().toArray(Entry[]::new);

        assertArray(entries, Entry.class);
    }

    @Test
    public void sourceList() {
        Object[] objects = streamList().toArray();

        assertArray(objects, Integer.class);
    }

    @Test
    public void sourceList_withArraySupplier() {
        Integer[] elements = streamList().toArray(Integer[]::new);

        assertArray(elements, Integer.class);
    }

}
