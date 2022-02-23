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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueIteratorTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test
    public void testIterator() {
        IQueue<VersionedObject<String>> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        Iterator<VersionedObject<String>> iterator = queue.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            VersionedObject<String> o = iterator.next();
            int itemId = i++;
            assertEquals(o, new VersionedObject<>("item" + itemId, itemId));
        }
    }

    @Test
    public void testIterator_whenQueueEmpty() {
        IQueue<VersionedObject<String>> queue = newQueue();
        Iterator<VersionedObject<String>> iterator = queue.iterator();

        assertFalse(iterator.hasNext());
        try {
            assertNull(iterator.next());
            fail();
        } catch (NoSuchElementException e) {
            ignore(e);
        }
    }

    @Test
    public void testIteratorRemove() {
        IQueue<VersionedObject<String>> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        Iterator<VersionedObject<String>> iterator = queue.iterator();
        iterator.next();
        try {
            iterator.remove();
            fail();
        } catch (UnsupportedOperationException e) {
            ignore(e);
        }

        assertEquals(10, queue.size());
    }

    private IQueue<VersionedObject<String>> newQueue() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getQueue(randomString());
    }
}
