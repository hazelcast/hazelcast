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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.monitor.LocalPNCounterStats;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

/**
 * Base implementation for simple {@link PNCounter} tests.
 * Concrete implementations must provide the two {@link HazelcastInstance}s
 * which will be used to perform the tests.
 */
public abstract class AbstractPNCounterBasicIntegrationTest extends HazelcastTestSupport {

    @Test
    public void testSimpleReplication() {
        final PNCounter counter1 = getInstance1().getPNCounter("counter");
        final PNCounter counter2 = getInstance2().getPNCounter("counter");

        assertEquals(5, counter1.addAndGet(5));

        assertCounterValueEventually(5, counter1);
        assertCounterValueEventually(5, counter2);
    }

    @Test
    public void statistics() {
        final PNCounter counter1 = getInstance1().getPNCounter("counterWithStats");
        final PNCounter counter2 = getInstance2().getPNCounter("counterWithStats");
        final int parallelism = 5;
        final int loopsPerThread = 100;
        final AtomicLong finalValue = new AtomicLong();

        final ArrayList<Future> futures = new ArrayList<Future>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            futures.add(spawn(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < loopsPerThread; j++) {
                        counter1.addAndGet(5);
                        finalValue.addAndGet(5);
                        counter2.addAndGet(-2);
                        finalValue.addAndGet(-2);
                    }
                }
            }));
        }
        FutureUtil.waitForever(futures);
        assertCounterValueEventually(finalValue.longValue(), counter1);
        assertCounterValueEventually(finalValue.longValue(), counter2);

        int increments = 0;
        int decrements = 0;
        for (HazelcastInstance member : getMembers()) {
            final PNCounterService service = getNodeEngineImpl(member).getService(PNCounterService.SERVICE_NAME);
            for (LocalPNCounterStats stats : service.getStats().values()) {
                increments += stats.getTotalIncrementOperationCount();
                decrements += stats.getTotalDecrementOperationCount();
            }
        }

        assertEquals(parallelism * loopsPerThread, increments);
        assertEquals(parallelism * loopsPerThread, decrements);
    }

    protected abstract HazelcastInstance getInstance1();

    protected abstract HazelcastInstance getInstance2();

    protected abstract HazelcastInstance[] getMembers();

    private void assertCounterValueEventually(final long expectedValue, final PNCounter counter) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedValue, counter.get());
            }
        });
    }
}
