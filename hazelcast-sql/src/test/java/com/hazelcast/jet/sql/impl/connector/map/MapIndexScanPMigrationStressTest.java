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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport.Row;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class MapIndexScanPMigrationStressTest extends JetTestSupport {
    private static final int ITEM_COUNT = 650_000;
    private static final String MAP_NAME = "map";

    private TestHazelcastFactory factory;
    private HazelcastInstance[] instances;
    private IMap<Integer, Integer> map;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        instances = new HazelcastInstance[4];
        for (int i = 0; i < instances.length - 1; i++) {
            instances[i] = factory.newHazelcastInstance(smallInstanceConfig());
        }
        map = instances[0].getMap(MAP_NAME);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    @Ignore // TODO: [sasha] un-ignore after IMDG engine removal
    public void stressTest_hash() throws InterruptedException {
        List<Row> expected = new ArrayList<>();
        for (int i = 0; i <= ITEM_COUNT / 4; i++) {
            map.put(i, 1);
            expected.add(new Row(i, 1));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.HASH, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(1500L);
        mutator.start();

        // Awful performance of such a query, but still a good load for test.
        assertRowsAnyOrder("SELECT * FROM " + MAP_NAME + " WHERE this = 1", expected);

        mutator.terminate();
        mutator.join();
    }

    @Test
    @Ignore // TODO: [sasha] un-ignore after IMDG engine removal
    public void stressTest_sorted() throws InterruptedException {
        List<Row> expected = new ArrayList<>();
        for (int i = 0; i <= ITEM_COUNT; i++) {
            map.put(i, i);
            expected.add(new Row(ITEM_COUNT - i, ITEM_COUNT - i));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(1000L);
        mutator.start();

        assertRowsOrdered("SELECT * FROM " + MAP_NAME + " ORDER BY this DESC", expected);

        mutator.terminate();
        mutator.join();
    }

    private class MutatorThread extends Thread {

        private volatile boolean active = true;
        private final long delay;

        private MutatorThread(long delay) {
            this.delay = delay;
        }

        private void terminate() {
            active = false;
        }

        @Override
        @SuppressWarnings("BusyWait")
        public void run() {
            while (active) {
                instances[3].shutdown();
                instances[3] = factory.newHazelcastInstance(smallInstanceConfig());
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) {
        List<Row> actualRows = new ArrayList<>();
        instances[0].getSql()
                .execute(sql)
                .iterator()
                .forEachRemaining(row -> actualRows.add(new Row(row.getObject(0), row.getObject(1))));

        assertThat(actualRows.size()).isEqualTo(expectedRows.size());
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    private void assertRowsOrdered(String sql, Collection<Row> expectedRows) {
        List<Row> actualRows = new ArrayList<>();
        instances[0].getSql()
                .execute(sql)
                .iterator()
                .forEachRemaining(row -> actualRows.add(new Row(row.getObject(0), row.getObject(1))));

        assertThat(actualRows.size()).isEqualTo(expectedRows.size());
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }
}
