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
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.*;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.TempData.DeleteEntryProcessor;
import static com.hazelcast.map.TempData.LoggingEntryProcessor;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EntryProcessorStressTest extends HazelcastTestSupport {

    @Test
    public void dropedEntryProcessorTest_withKeyOwningNodeTermination() throws ExecutionException, InterruptedException {
        String mapName = randomString();
        Config cfg = new Config();
        cfg.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);


        final int maxItterations=50;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(maxItterations+1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);

        for(int iteration=0; iteration<maxItterations; iteration++){

            System.out.println("======================"+iteration+"==============================");

            HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

            final int maxTasks = 20;
            final Object key  =  generateKeyOwnedBy(instance2);

            final IMap<Object, List<Integer>> processorMap = instance1.getMap(mapName);
            processorMap.put(key, new ArrayList<Integer>());

            List<Future> futures = new ArrayList<Future>();

            for (int i = 0 ; i < maxTasks ; i++) {
                Future f = processorMap.submitToKey(key, new SimpleEntryProcessor(i));
                futures.add(f);

                if(i==maxTasks/2){
                    instance2.getLifecycleService().terminate();
                }
            }

            final int itter = iteration;
            assertTrueEventually(new AssertTask() {
                public void run() throws Exception {
                    List<Integer> actualOrder = processorMap.get(key);
                    System.out.println("asserting = " + actualOrder + " size " + actualOrder.size());
                    //using >= for the test, as it can be the case that an entry processor could be executed more the once
                    //when the owning node is terminated after running the entry processor (and the backup) but before the response is sent
                    assertTrue("failed to execute all entry processor tasks at iteration", actualOrder.size() >= maxTasks);
                }
            });
        }
    }

    private static class SimpleEntryProcessor implements DataSerializable, EntryProcessor<Object, List<Integer>>, EntryBackupProcessor<Object, List<Integer>> {
        private Integer id;
        private boolean backup=false;

        public SimpleEntryProcessor() {}

        public SimpleEntryProcessor(Integer id) {
            this.id = id;
        }

        @Override
        public Object process(Map.Entry<Object, List<Integer>> entry) {
            List l = entry.getValue();
            l.add(id);

            if(backup){
                System.out.print("Backup ");
            }
            System.out.println("EntryProcessor => "+l +" size="+l.size()+" last val="+id);
            return id;
        }

        @Override
        public void processBackup(Map.Entry entry) {
            backup=true;
            process(entry);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readObject();
        }

        @Override
        public EntryBackupProcessor<Object, List<Integer>> getBackupProcessor() {
            return this;
        }
    }
}