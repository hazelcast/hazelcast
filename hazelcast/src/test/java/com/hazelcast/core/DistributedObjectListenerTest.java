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

package com.hazelcast.core;

import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.map.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DistributedObjectListenerTest extends HazelcastTestSupport {

    @After
    public void teardown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testDestroyJustAfterCreate() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        instance.addDistributedObjectListener(new EventCountListener());
        IMap<Object, Object> map = instance.getMap(randomString());
        map.destroy();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, EventCountListener.createdCount.get());
                Assert.assertEquals(1, EventCountListener.destroyedCount.get());
                Collection<DistributedObject> distributedObjects = instance.getDistributedObjects()
                        .stream()
                        .filter(obj -> !obj.getName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX))
                        .collect(Collectors.toList());
                Assert.assertTrue(distributedObjects.isEmpty());
            }
        };
        assertTrueEventually(task, 5);
        assertTrueAllTheTime(task, 3);
    }

    public static class EventCountListener implements DistributedObjectListener {

        public static AtomicInteger createdCount = new AtomicInteger();
        public static AtomicInteger destroyedCount = new AtomicInteger();

        public void distributedObjectCreated(DistributedObjectEvent event) {
            createdCount.incrementAndGet();
        }

        public void distributedObjectDestroyed(DistributedObjectEvent event) {
            destroyedCount.incrementAndGet();
        }
    }
}
