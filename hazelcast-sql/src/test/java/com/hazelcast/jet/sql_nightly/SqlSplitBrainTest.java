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

package com.hazelcast.jet.sql_nightly;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetSplitBrainTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class SqlSplitBrainTest extends JetSplitBrainTestSupport {

    @Override
    protected void onConfigCreated(Config config) {
        config.getJetConfig().setBackupCount(MAX_BACKUP_COUNT);
        config.getJetConfig().setScaleUpDelayMillis(3000);
    }

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/19472
    public void test_indexScan() throws InterruptedException {
        Thread[] threads = new Thread[16];
        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger numQueries = new AtomicInteger();

        Consumer<HazelcastInstance[]> beforeSplit = instances -> {
            IMap<Integer, Integer> m = instances[0].getMap("m");
            for (int i = 0; i < 10_000; i++) {
                m.put(i, i);
            }
            m.addIndex(new IndexConfig().addAttribute("this"));
            SqlTestSupport.createMapping(instances[0], "m", Integer.class, Integer.class);

            for (int i = 0, threadsLength = threads.length; i < threadsLength; i++) {
                HazelcastInstance inst = createHazelcastClient();
                threads[i] = new Thread(() -> {
                    int numQueriesLocal = 0;
                    while (!done.get()) {
                        try {
                            //noinspection StatementWithEmptyBody
                            for (SqlRow ignored : inst.getSql().execute("select * from m where this>100 and this<1000")) {
                                // do nothing
                            }
                        } catch (Throwable e) {
                            logger.info(e);
                        }
                        numQueriesLocal++;
                    }
                    numQueries.addAndGet(numQueriesLocal);
                });
                threads[i].start();

                sleepSeconds(1);
            }
        };

        testSplitBrain(1, 1, beforeSplit, null, null);

        done.set(true);
        boolean stuck = false;
        for (Thread t : threads) {
            t.join(1000);
            if (t.isAlive()) {
                logger.info("thread " + t + " stuck");
                stuck = true;
            }
        }

        logger.info("num queries executed: " + numQueries.get());

        if (stuck) {
            fail("some threads were stuck");
        }
    }
}
