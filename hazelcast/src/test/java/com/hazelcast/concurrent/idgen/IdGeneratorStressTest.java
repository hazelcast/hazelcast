/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.LongHashSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class IdGeneratorStressTest extends HazelcastTestSupport {

    private static final int THREAD_COUNT = 32;

    private static final int NUMBER_OF_IDS_PER_THREAD = 40000;

    private static final int TOTAL_ID_GENERATED = THREAD_COUNT * NUMBER_OF_IDS_PER_THREAD;

    @Parameterized.Parameter(0)
    public int clusterSize;

    @Parameterized.Parameters(name = "clusterSize:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1},
                {6},
        });
    }

    String name;
    HazelcastInstance[] instances;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        instances = factory.newInstances();
        name = randomString();
    }

    @Test
    public void testMultipleThreads() throws ExecutionException, InterruptedException {
        pickIdGenerator().init(13013);

        List<Future> futureList = new ArrayList<Future>(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            IdGenerator idGenerator = pickIdGenerator();
            IdGeneratorCallable callable = new IdGeneratorCallable(idGenerator);
            Future<long[]> future = spawn(callable);
            futureList.add(future);
        }

        LongHashSet totalGeneratedIds = new LongHashSet(TOTAL_ID_GENERATED, -1);
        for (Future<long[]> future : futureList) {
            long[] generatedIds = future.get();
            for (long generatedId : generatedIds) {
                assertTrue("ID: " + generatedId, totalGeneratedIds.add(generatedId));
            }
        }

        assertEquals(TOTAL_ID_GENERATED, totalGeneratedIds.size());
    }

    private IdGenerator pickIdGenerator() {
        int index = RandomPicker.getInt(instances.length);
        return instances[index].getIdGenerator(name);
    }

    private static class IdGeneratorCallable implements Callable<long[]> {

        IdGenerator idGenerator;

        public IdGeneratorCallable(IdGenerator idGenerator) {
            this.idGenerator = idGenerator;
        }

        @Override
        public long[] call() throws Exception {
            long[] generatedIds = new long[NUMBER_OF_IDS_PER_THREAD];
            for (int j = 0; j < NUMBER_OF_IDS_PER_THREAD; j++) {
                generatedIds[j] = idGenerator.newId();
            }
            return generatedIds;
        }
    }

}
