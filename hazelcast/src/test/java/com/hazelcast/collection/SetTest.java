/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SetTest extends HazelcastTestSupport {

    @Test
    public void testCollectionItem_equalsAndHash(){
        SerializationServiceBuilder serializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService build = serializationServiceBuilder.build();
        Data value = build.toData(randomString());
        CollectionItem firstItem = new CollectionItem(1, value);
        CollectionItem secondItem = new CollectionItem(2, value);
        assertTrue(firstItem.equals(secondItem));
        assertEquals(firstItem.hashCode(), secondItem.hashCode());
    }

    //    ======================== isEmpty test ============================

    @Test
    public void testIsEmpty_whenEmpty() {
        ISet set = newSet();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        ISet set = newSet();
        set.add("item1");
        assertFalse(set.isEmpty());
    }

    //    ======================== add - addAll test =======================

    @Test
    public void testAdd() {
        ISet set = newSet();
        for (int i = 1; i <= 10; i++) {
            assertTrue(set.add("item" + i));
        }
        assertEquals(10, set.size());
    }

    @Test
    public void testAdd_withMaxCapacity() {
        ISet set = newSetWithMaxSize(1);
        set.add("item");
        for (int i = 1; i <= 10; i++) {
            assertFalse(set.add("item" + i));
        }
        assertEquals(1, set.size());
    }

    @Test(expected = NullPointerException.class)
    public void testAddNull() {
        ISet set = newSet();
        set.add(null);
    }

    @Test
    public void testAddAll_Basic() {
        Set added = new HashSet();
        ISet set = newSet();
        added.add("item1");
        added.add("item2");
        set.addAll(added);
        assertEquals(2, set.size());
    }

    @Test
    public void testAddAll_whenAllElementsSame() {
        Set added = new HashSet();
        ISet set = newSet();
        for (int i = 1; i <= 10; i++) {
            added.add("item");
        }
        set.addAll(added);
        assertEquals(1, set.size());
    }

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        Set added = new HashSet();
        ISet set = newSet();
        added.add("item1");
        added.add(null);
        try {
            assertFalse(set.addAll(added));
        } catch (NullPointerException e) {
        }
        assertEquals(0, set.size());
    }

    //    ======================== remove  - removeAll ==========================


    @Test
    public void testRemoveBasic() {
        ISet set = newSet();
        set.add("item1");
        assertTrue(set.remove("item1"));
        assertEquals(0, set.size());
    }

    @Test
    public void testRemove_whenElementNotExist() {
        ISet set = newSet();
        set.add("item1");
        assertFalse(set.remove("notExist"));
        assertEquals(1, set.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_whenArgumentNull() {
        ISet set = newSet();
        set.remove(null);
    }

    @Test
    public void testRemoveAll() {
        ISet set = newSet();
        Set removed = new HashSet();
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            removed.add("item" + i);
        }
        set.removeAll(removed);
        assertEquals(0, set.size());
    }

    //    ======================== iterator ==========================
    @Test
    public void testIterator() {
        ISet set = newSet();
        set.add("item");
        Iterator iterator = set.iterator();
        assertEquals("item", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = UnsupportedOperationException.class)
    @ClientCompatibleTest
    public void testIteratorRemoveThrowsUnsupportedOperationException() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        ISet set = instance1.getSet("defSet");
        set.add("item");

        Iterator iterator = set.iterator();
        iterator.next();
        iterator.remove();
    }

    //    ======================== clear ==========================

    @Test
    public void testClear() {
        ISet set = newSet();
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
        }
        assertEquals(10, set.size());
        set.clear();
        assertEquals(0, set.size());
    }

    @Test
    public void testClear_whenSetEmpty() {
        ISet set = newSet();
        set.clear();
        assertEquals(0, set.size());
    }


    //    ======================== retainAll ==========================

    @Test
    public void testRetainAll_whenArgumentEmptyCollection() {
        ISet set = newSet();
        Set retained = new HashSet();

        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
        }
        set.retainAll(retained);
        assertEquals(0, set.size());
    }

    @Test
    public void testRetainAll_whenArgumentHasSameElements() {
        ISet set = newSet();
        Set retained = new HashSet();

        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            retained.add("item" + i);
        }
        set.retainAll(retained);
        assertEquals(10, set.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRetainAll_whenCollectionNull() {
        ISet set = newSet();
        Set retained = null;
        set.retainAll(retained);
        assertEquals(0, set.size());
    }

    //    ======================== contains - containsAll ==========================
    @Test
    public void testContains() {
        ISet set = newSet();
        set.add("item1");
        assertTrue(set.contains("item1"));
    }

    @Test
    public void testContains_whenEmpty() {
        ISet set = newSet();
        assertFalse(set.contains("notExist"));
    }

    @Test
    public void testContains_whenNotContains() {
        ISet set = newSet();
        set.add("item1");
        assertFalse(set.contains("notExist"));
    }

    @Test
    public void testContainsAll() {
        ISet set = newSet();
        Set contains = new HashSet();

        contains.add("item1");
        contains.add("item2");
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            contains.add("item" + i);
        }
        assertTrue(set.containsAll(contains));
    }

    @Test
    public void testContainsAll_whenSetNotContains() {
        ISet set = newSet();

        Set contains = new HashSet();
        contains.add("item1");
        contains.add("item100");
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            contains.add("item" + i);
        }
        assertFalse(set.containsAll(contains));

    }

    //    ======================== helper methods ==========================
    protected ISet newSet() {
        HazelcastInstance instance = createHazelcastInstance();
        return instance.getSet(randomString());
    }

    protected ISet newSetWithMaxSize(int maxSize) {
        String name = randomString();
        Config config = new Config();
        SetConfig setConfig = config.getSetConfig(name);
        setConfig.setMaxSize(maxSize);
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getSet(name);
    }
}
