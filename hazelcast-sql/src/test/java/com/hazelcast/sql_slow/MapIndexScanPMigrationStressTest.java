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

package com.hazelcast.sql_slow;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport.Row;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapIndexScanPMigrationStressTest extends JetTestSupport {
    private static final int ITEM_COUNT = 500_000;
    private static final String MAP_NAME = "map";

    private AtomicReference<Throwable> mutatorException;
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
        mutatorException = new AtomicReference<>(null);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test(timeout = 600_000)
    public void stressTest_hash() throws InterruptedException {
        List<Row> expected = new ArrayList<>();
        for (int i = 0; i <= ITEM_COUNT / 5; i++) {
            map.put(i, 1);
            expected.add(new Row(i, 1));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.HASH, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(2000L);

        // Awful performance of such a query, but still a good load for test.
        assertRowsAnyOrder("SELECT * FROM " + MAP_NAME + " WHERE this = 1", expected, mutator);

        mutator.terminate();
        mutator.join();
        assertThat(mutatorException.get()).isNull();
    }

    @Test(timeout = 600_000)
    public void stressTest_sorted() throws InterruptedException {
        List<Row> expected = new ArrayList<>();
        for (int i = 0; i <= ITEM_COUNT; i++) {
            map.put(i, i);
            expected.add(new Row(ITEM_COUNT - i, ITEM_COUNT - i));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(2000L);
        assertRowsOrdered("SELECT * FROM " + MAP_NAME + " ORDER BY this DESC", expected, mutator);

        mutator.terminate();
        mutator.join();
        assertThat(mutatorException.get()).isNull();
    }

    private class MutatorThread extends Thread {
        private boolean firstLaunch = true;
        private boolean active = true;
        private final long delay;

        private MutatorThread(long delay) {
            this.delay = delay;
        }

        private synchronized void terminate() {
            active = false;
        }

        @Override
        @SuppressWarnings("BusyWait")
        public void run() {
            while (active) {
                try {
                    if (!firstLaunch) {
                        instances[3].shutdown();
                    } else {
                        firstLaunch = false;
                    }
                    instances[3] = factory.newHazelcastInstance(smallInstanceConfig());

                    Thread.sleep(delay);
                } catch (Exception e) {
                    mutatorException.set(e);
                    e.printStackTrace();
                }
            }
        }
    }

    private void assertRowsAnyOrder(String sql, Collection<Row> expectedRows, Thread mutator) {
        List<Row> actualRows = executeAndGetResult(sql, mutator);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    private void assertRowsOrdered(String sql, Collection<Row> expectedRows, Thread mutator) {
        List<Row> actualRows = executeAndGetResult(sql, mutator);
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }

    private List<Row> executeAndGetResult(String sql, Thread mutator) {
        List<Row> actualRows = new ArrayList<>();

        Iterator<SqlRow> rowIterator = instances[0].getSql()
                .execute(sql)
                .iterator();

        assertThat(rowIterator.hasNext()).isTrue();
        assertJobExecuting(instances[0].getJet().getJobs().get(0), instances[0]);

        mutator.start();

        rowIterator.forEachRemaining(row -> actualRows.add(new Row(row.getObject(0), row.getObject(1))));

        return actualRows;
    }
}
