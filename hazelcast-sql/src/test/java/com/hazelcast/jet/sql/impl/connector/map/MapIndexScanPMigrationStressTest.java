/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.SqlTestSupport.assertRowsOrdered;

public class MapIndexScanPMigrationStressTest extends SimpleTestInClusterSupport {
    static final int ITEM_COUNT = 5000;
    static final String MAP_NAME = "map";

    private IMap<Integer, Integer> map;

    @BeforeClass
    public static void setUpClass() {
        initializeExceptLast(3, smallInstanceConfig());
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Test
//    @Ignore // TODO: [sasha] un-ignore after IMDG engine removal
    public void stressTestSameOrder() {
        List<SqlTestSupport.Row> expected = new ArrayList<>();
        for (int i = 0; i <= ITEM_COUNT; i++) {
            map.put(i, i);
            expected.add(new SqlTestSupport.Row(i, i));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(instances(), instances().length - 1);
        mutator.start();

        assertRowsOrdered("SELECT * FROM " + MAP_NAME, expected);

        try {
            mutator.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static class MutatorThread extends Thread {
        private final HazelcastInstance[] instances;
        private final int iterations;
        private final long delay;

        MutatorThread(HazelcastInstance[] instances, int iterations) {
            super();
            this.instances = instances;
            this.iterations = iterations;
            this.delay = 2000L;
        }

        MutatorThread(HazelcastInstance[] instances, int iterations, long delay) {
            super();
            this.instances = instances;
            this.iterations = iterations;
            this.delay = delay;
        }

        @Override
        public void run() {
            for (int i = 0; i < iterations; ++i) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (i != 0) {
                    instances[instances.length - 1].shutdown();
                }
                instances[instances.length - 1] = factory().newHazelcastInstance(smallInstanceConfig());
                System.out.println("Instance was initialized.");
            }
        }
    }
}
