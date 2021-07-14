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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.SqlTestSupport.Row;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class MapIndexScanPMigrationStressTest extends JetTestSupport {

    private static final int ITEM_COUNT = 10_000;
    private static final String MAP_NAME = "map";

    private final AtomicBoolean requesterFinished = new AtomicBoolean(false);
    private TestHazelcastFactory factory;
    private HazelcastInstance[] instances;
    private IMap<Integer, Integer> map;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        instances = new HazelcastInstance[3];
        for (int i = 0; i < instances.length; i++) {
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
        for (int i = 0; i <= ITEM_COUNT; i++) {
            map.put(i, i);
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.HASH, "this").setName(randomName());
        map.addIndex(indexConfig);

        MutatorThread mutator = new MutatorThread(50L);
        QueryThread requester = new QueryThread(
                () -> {
                    int i = new Random().nextInt(ITEM_COUNT);
                    return Tuple2.tuple2("SELECT * FROM " + MAP_NAME + " WHERE this = " + i, singletonList(new Row(i, i)));
                },
                500,
                10L
        );
        mutator.start();
        requester.start();
        requester.join();
        requesterFinished.set(true);
        mutator.join();

        assertThat(requester.allQueriesSucceeded()).isTrue();
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

        MutatorThread mutator = new MutatorThread(10L);
        QueryThread requester = new QueryThread(
                () -> Tuple2.tuple2("SELECT * FROM " + MAP_NAME + " ORDER BY this DESC", expected),
                500,
                10L
        );
        mutator.start();
        requester.start();
        requester.join();
        requesterFinished.set(true);
        mutator.join();

        assertThat(requester.allQueriesSucceeded()).isTrue();
    }

    private class MutatorThread extends Thread {
        private final long delay;

        private MutatorThread(long delay) {
            this.delay = delay;
        }

        @Override
        public void run() {
            while (!requesterFinished.get()) {
                int index = 1 + ThreadLocalRandom.current().nextInt(instances.length - 1);
                factory.terminate(instances[index]);
                instances[index] = factory.newHazelcastInstance(smallInstanceConfig());
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class QueryThread extends Thread {

        private final Supplier<Tuple2<String, List<Row>>> sqlAndExpectedRowsSupplier;
        private final int iterations;
        private final long delay;

        private boolean result = true;

        private QueryThread(Supplier<Tuple2<String, List<Row>>> sqlAndExpectedRowsSupplier, int iterations, long delay) {
            this.sqlAndExpectedRowsSupplier = sqlAndExpectedRowsSupplier;
            this.iterations = iterations;
            this.delay = delay;
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public void run() {
            int actualIterations = 0;
            try {
                for (int i = 0; i < iterations; ++i) {
                    Tuple2<String, List<Row>> sqlAndExpectedRows = sqlAndExpectedRowsSupplier.get();
                    List<Row> actualRows = new ArrayList<>();
                    instances[0].getSql()
                            .execute(sqlAndExpectedRows.f0())
                            .iterator()
                            .forEachRemaining(row -> actualRows.add(new Row(row.getObject(0), row.getObject(1))));
                    boolean tempResult = sqlAndExpectedRows.f1().equals(actualRows);
                    result &= tempResult;

                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (HazelcastSqlException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                result = false;
            } finally {
                assertThat(actualIterations).isEqualTo(iterations);
            }
        }

        private boolean allQueriesSucceeded() {
            return result;
        }
    }
}
