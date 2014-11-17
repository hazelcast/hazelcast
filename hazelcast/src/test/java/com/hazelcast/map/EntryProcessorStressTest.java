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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class EntryProcessorStressTest extends HazelcastTestSupport {

    @Test
    @Ignore // https://github.com/hazelcast/hazelcast/issues/3683
    public void dropedEntryProcessorTest_withKeyOwningNodeTermination() throws ExecutionException, InterruptedException {
        String mapName = randomString();
        Config cfg = new Config();
        cfg.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);

        final int maxIterations = 50;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(maxIterations+1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

            final int maxTasks = 20;
            final Object key = generateKeyOwnedBy(instance2);

            final IMap<Object, List<Integer>> processorMap = instance1.getMap(mapName);
            processorMap.put(key, new ArrayList<Integer>());

            for (int i = 0; i < maxTasks; i++) {
                processorMap.submitToKey(key, new SimpleEntryProcessor(i));

                if (i == maxTasks / 2) {
                    instance2.getLifecycleService().shutdown();
                }
            }

            assertTrueEventually(new AssertTask() {
                public void run() throws Exception {
                    List<Integer> actualOrder = processorMap.get(key);
                    //using >= for the test, as it can be the case that an entry processor could be executed more the once
                    //when the owning node is terminated after running the entry processor (and the backup) but before the response is sent
                    assertTrue("failed to execute all entry processor tasks at iteration",
                            actualOrder.size() >= maxTasks);
                }
            }, 30);
        }
    }

    private static class SimpleEntryProcessor implements DataSerializable, EntryProcessor<Object, List<Integer>>,
            EntryBackupProcessor<Object, List<Integer>> {

        private int id;

        public SimpleEntryProcessor() {}

        public SimpleEntryProcessor(Integer id) {
            this.id = id;
        }

        @Override
        public Object process(Map.Entry<Object, List<Integer>> entry) {
            List<Integer> list = entry.getValue();
            list.add(id);
            // add a random artificial latency
            LockSupport.parkNanos((long) (Math.random() * 10000));
            entry.setValue(list);
            return id;
        }

        @Override
        public void processBackup(Map.Entry<Object, List<Integer>> entry) {
            process(entry);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readInt();
        }

        @Override
        public EntryBackupProcessor<Object, List<Integer>> getBackupProcessor() {
            return this;
        }
    }
}
