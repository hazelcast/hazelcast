/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertEqualsEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public abstract class EventloopTest {

    private Reactor reactor;

    public abstract ReactorBuilder newReactorBuilder();

    @Before
    public void before() {
        ReactorBuilder reactorBuilder = newReactorBuilder();
        reactor = reactorBuilder.build().start();
    }

    @After
    public void after() {
        terminate(reactor);
    }

    @Test
    public void test_schedule() {
        Task task = new Task();

        reactor.offer(() -> reactor.eventloop.schedule(task, 1, SECONDS));

        assertTrueEventually(() -> assertEquals(1, task.count.get()));
    }

    private static final class Task implements Runnable {
        private final AtomicLong count = new AtomicLong();

        @Override
        public void run() {
            count.incrementAndGet();
        }
    }

    @Test
    public void test_sleep() {
        AtomicInteger executedCount = new AtomicInteger();
        long startMs = System.currentTimeMillis();
        reactor.offer(() -> reactor.eventloop().sleep(1, SECONDS)
                .then((o, ex) -> executedCount.incrementAndGet()));

        assertEqualsEventually(1, executedCount);
        long duration = System.currentTimeMillis() - startMs;
        System.out.println("duration:" + duration + " ms");
    }
}
