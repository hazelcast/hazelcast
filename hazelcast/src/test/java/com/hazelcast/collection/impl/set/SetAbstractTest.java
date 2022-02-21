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

package com.hazelcast.collection.impl.set;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.collection.ISet;
import com.hazelcast.transaction.TransactionalSet;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class SetAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;

    private ISet<String> set;
    private Config config;
    private HazelcastInstance local;
    private HazelcastInstance target;

    @Before
    public void setup() {
        config = new Config();
        config.addSetConfig(new SetConfig("testAdd_withMaxCapacity*").setMaxSize(1));

        instances = newInstances(config);
        local = instances[0];
        target = instances[instances.length - 1];
        String methodName = getTestMethodName();
        String name = randomNameOwnedBy(target, methodName);
        set = local.getSet(name);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    //    ======================== isEmpty test ============================

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(set.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        set.add("item1");
        assertFalse(set.isEmpty());
    }

    //    ======================== add - addAll test =======================

    @Test
    public void testAdd() {
        for (int i = 1; i <= 10; i++) {
            assertTrue(set.add("item" + i));
        }
        assertEquals(10, set.size());
    }

    @Test
    public void testAdd_withMaxCapacity() {
        String name = randomNameOwnedBy(target, "testAdd_withMaxCapacity");
        set = local.getSet(name);
        set.add("item");
        for (int i = 1; i <= 10; i++) {
            assertFalse(set.add("item" + i));
        }
        assertEquals(1, set.size());
    }

    @Test(expected = NullPointerException.class)
    public void testAddNull() {
        set.add(null);
    }

    @Test
    public void testAddAll_Basic() {
        Set<String> added = new HashSet<String>();
        added.add("item1");
        added.add("item2");
        set.addAll(added);
        assertEquals(2, set.size());
    }

    @Test
    public void testAddAll_whenAllElementsSame() {
        Set<String> added = new HashSet<String>();
        for (int i = 1; i <= 10; i++) {
            added.add("item");
        }
        set.addAll(added);
        assertEquals(1, set.size());
    }

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        Set<String> added = new HashSet<String>();
        added.add("item1");
        added.add(null);
        try {
            assertFalse(set.addAll(added));
        } catch (NullPointerException e) {
            ignore(e);
        }
        assertEquals(0, set.size());
    }

    //    ======================== remove  - removeAll ==========================

    @Test
    public void testRemoveBasic() {
        set.add("item1");
        assertTrue(set.remove("item1"));
        assertEquals(0, set.size());
    }

    @Test
    public void testRemove_whenElementNotExist() {
        set.add("item1");
        assertFalse(set.remove("notExist"));
        assertEquals(1, set.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_whenArgumentNull() {
        set.remove(null);
    }

    @Test
    public void testRemoveAll() {
        Set<String> removed = new HashSet<String>();
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
        set.add("item");
        Iterator iterator = set.iterator();
        assertEquals("item", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorRemoveThrowsUnsupportedOperationException() {
        set.add("item");

        Iterator iterator = set.iterator();
        iterator.next();
        iterator.remove();
    }

    //    ======================== clear ==========================

    @Test
    public void testClear() {
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
        }
        assertEquals(10, set.size());
        set.clear();
        assertEquals(0, set.size());
    }

    @Test
    public void testClear_whenSetEmpty() {
        set.clear();
        assertEquals(0, set.size());
    }

    //    ======================== retainAll ==========================

    @Test
    public void testRetainAll_whenArgumentEmptyCollection() {
        Set<String> retained = emptySet();

        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
        }
        set.retainAll(retained);
        assertEquals(0, set.size());
    }

    @Test
    public void testRetainAll_whenArgumentHasSameElements() {
        Set<String> retained = new HashSet<String>();

        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            retained.add("item" + i);
        }
        set.retainAll(retained);
        assertEquals(10, set.size());
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testRetainAll_whenCollectionNull() {
        set.retainAll(null);
    }

    //    ======================== contains - containsAll ==========================
    @Test
    public void testContains() {
        set.add("item1");
        assertContains(set, "item1");
    }

    @Test
    public void testContains_whenEmpty() {
        assertNotContains(set, "notExist");
    }

    @Test
    public void testContains_whenNotContains() {
        set.add("item1");
        assertNotContains(set, "notExist");
    }

    @Test
    public void testContainsAll() {
        Set<String> contains = new HashSet<String>();

        contains.add("item1");
        contains.add("item2");
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            contains.add("item" + i);
        }
        assertContainsAll(set, contains);
    }

    @Test
    public void testContainsAll_whenSetNotContains() {
        Set<String> contains = new HashSet<String>();
        contains.add("item1");
        contains.add("item100");
        for (int i = 1; i <= 10; i++) {
            set.add("item" + i);
            contains.add("item" + i);
        }
        assertNotContainsAll(set, contains);
    }

    @Test
    public void testNameBasedAffinity() {
        //creates more instances to increase a chance 'foo' will not be owned by
        //the same member as 'foo@1'
        newInstances(config);
        newInstances(config);

        int numberOfSets = 100;

        ISet[] localSets = new ISet[numberOfSets];
        ISet[] targetSets = new ISet[numberOfSets];

        for (int i = 0; i < numberOfSets; i++) {
            String name = randomName() + "@" + i;
            localSets[i] = local.getSet(name);
            targetSets[i] = target.getSet(name);
        }

        for (final ISet set : localSets) {
            TransactionalTask task = new TransactionalTask() {
                @Override
                public Object execute(TransactionalTaskContext context) throws TransactionException {
                    TransactionalSet<String> txSet = context.getSet(set.getName());
                    txSet.add("Hello");
                    return null;
                }
            };
            local.executeTransaction(task);
        }

        for (ISet set : localSets) {
            assertEquals(1, set.size());
        }
    }
}
