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
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

<<<<<<<HEAD
        =======
        >>>>>>>unit tests for ISet

/**
 * @author ali 3/6/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SetTest extends HazelcastTestSupport {

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
        set.add("item2");
        Set setTest = new HashSet();
        setTest.add("item1");
        setTest.add("item2");
        assertFalse(set.isEmpty());
        set.addItemListener(new ItemListener() {
            @Override
            public void itemAdded(ItemEvent item) {
                System.out.println(item.getItem());
            }

            @Override
            public void itemRemoved(ItemEvent item) {
                System.out.println(item.getItem());
            }
        },false);
        set.add("asdasdasd");
        set.removeAll(setTest);
    }

    //    ======================== add - addAll test =======================

    @Test
    public void testAdd() {
        ISet set = newSet();
        for (int i = 1; i <= 10; i++) {
            assertTrue(set.add("item" + i));
        }
        assertSizeEventually(10, set);
    }

    @Test
    public void testAdd_withMaxCapacity() {
        ISet set = newSetWithMaxSize(1);
        set.add("item");
        for (int i = 1; i <= 10; i++) {
            assertFalse(set.add("item" + i));
        }
        assertSizeEventually(1, set);
    }

    @Test(expected = NullPointerException.class)
    public void testAddNull() {
        ISet set = newSet();
        set.add(null);
    }

    @Test
    public void testAddAll_Basic() {
        Set setTest = new HashSet();
        ISet set = newSet();
        setTest.add("item1");
        setTest.add("item2");
        set.addAll(setTest);
        assertSizeEventually(2, set);
    }

    @Test
    public void testAddAll_whenAllElementsSame() {
        Set setTest = new HashSet();
        ISet set = newSet();
        for (int i = 1; i <= 10; i++) {
            setTest.add("item");
        }
        set.addAll(setTest);
        assertSizeEventually(1, set);
    }

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        Set setTest = new HashSet();
        ISet set = newSet();
        setTest.add("item1");
        setTest.add(null);
        try {
            assertFalse(set.addAll(setTest));
        } catch (NullPointerException e) {
        }
        assertSizeEventually(0, set);
    }

    //    ======================== remove  - removeAll ==========================


    @Test
    public void testRemoveBasic() {
        ISet set = newSet();
        set.add("item1");
        assertTrue(set.remove("item1"));
        assertSizeEventually(0, set);
    }

    @Test
    public void testRemove_whenElementNotExist() {
        ISet set = newSet();
        set.add("item1");
        assertFalse(set.remove("notExist"));
        assertSizeEventually(1, set);
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_whenArgumentNull() {
        ISet set = newSet();
        assertFalse(set.remove(null));
    }

    @Test
    public void testRemoveAll() {
        ISet set = newSet();
        Set setTest = new HashSet();
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            setTest.add("item" + i);
        }
        set.removeAll(setTest);
        assertSizeEventually(0, set);
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

    // this is a bug, because when we call iterator.remove, according to javadoc
    // we need to remove this element from collection or throw illegal operation exception
    @Ignore
    @Test
    public void testIteratorRemove() {
        ISet set = newSet();
        set.add("item");
        Iterator iterator = set.iterator();
        iterator.next();
        iterator.remove();
        assertSizeEventually(1, set);
    }

    //    ======================== clear ==========================

    @Test
    public void testClear() {
        ISet set = newSet();
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
        }
        assertSizeEventually(10, set);
        set.clear();
        assertSizeEventually(0, set);
    }

    //    ======================== retainAll ==========================

    @Test
    public void testRetainAll_whenArgumentEmptyCollection() {
        ISet set = newSet();
        Set setTest = new HashSet();

        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
        }
        set.retainAll(setTest);
        assertSizeEventually(0, set);
    }

    @Test
    public void testRetainAll_whenArgumentHasSameElements() {
        ISet set = newSet();
        Set setTest = new HashSet();

        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            setTest.add("item" + i);
        }
        set.retainAll(setTest);
        assertSizeEventually(10, set);
    }

    //    ======================== contains - containsAll ==========================
    @Test
    public void testContains() {
        ISet set = newSet();
        set.add("item1");
        assertTrue(set.contains("item1"));
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
        Set setTest = new HashSet();

        setTest.add("item1");
        setTest.add("item2");
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            setTest.add("item" + i);
        }
        assertTrue(set.containsAll(setTest));
    }

    @Test
    public void testContainsAll_whenSetNotContains() {
        ISet set = newSet();
        Set setTest = new HashSet();

        setTest.add("item1");
        setTest.add("item100");
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            setTest.add("item" + i);
        }
        assertFalse(set.containsAll(setTest));

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
