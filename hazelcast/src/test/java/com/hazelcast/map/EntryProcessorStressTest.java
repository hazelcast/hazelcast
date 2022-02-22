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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class EntryProcessorStressTest extends HazelcastTestSupport {

    private static final int MAX_ITERATIONS = 50;
    private static final int MAX_TASKS = 20;

    @Test
    public void droppedEntryProcessorTest_withKeyOwningNodeTermination() throws Exception {
        String mapName = randomString();

        Config config = new Config();
        config.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(MAX_ITERATIONS + 1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            HazelcastInstance instance2 = factory.newHazelcastInstance(config);
            final Object key = generateKeyOwnedBy(instance2);

            final IMap<Object, List<Integer>> processorMap = instance1.getMap(mapName);
            processorMap.put(key, new ArrayList<Integer>());

            for (int i = 0; i < MAX_TASKS; i++) {
                processorMap.submitToKey(key, new SimpleEntryProcessor(i));

                if (i == MAX_TASKS / 2) {
                    instance2.shutdown();
                }
            }

            assertTrueEventually(new AssertTask() {
                public void run() throws Exception {
                    // using >= for the test, as it can be the case that an EntryProcessor could be executed more than once,
                    // when the owning node is terminated after running the EntryProcessor (and the backup),
                    // but before the response is sent
                    List<Integer> actualOrder = processorMap.get(key);
                    assertTrue("failed to execute all entry processor tasks at iteration", actualOrder.size() >= MAX_TASKS);
                }
            }, 30);
        }
    }

    private static class SimpleEntryProcessor implements DataSerializable,
            EntryProcessor<Object, List<Integer>, Integer> {

        private int id;

        SimpleEntryProcessor() {
        }

        SimpleEntryProcessor(Integer id) {
            this.id = id;
        }

        @Override
        public Integer process(Map.Entry<Object, List<Integer>> entry) {
            List<Integer> list = entry.getValue();
            list.add(id);
            // add a random artificial latency
            LockSupport.parkNanos((long) (Math.random() * 10000));
            entry.setValue(list);
            return id;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readInt();
        }
    }
}
