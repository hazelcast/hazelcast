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

package com.hazelcast.internal.tpc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class Eventloop_Unsafe_Test {

    private Eventloop eventloop;

    public abstract Eventloop create();

    @Before
    public void before() {
        eventloop = create();
        eventloop.start();
    }

    @After
    public void after() {
        eventloop.shutdown();
    }


    @Test
    public void test_sleep() {
        AtomicInteger executedCount = new AtomicInteger();
        long startMs = System.currentTimeMillis();
        eventloop.offer(() -> eventloop.unsafe().sleep(1, SECONDS)
                .then((o, ex) -> executedCount.incrementAndGet()));

        assertEqualsEventually(1, executedCount);
        long duration = System.currentTimeMillis() - startMs;
        System.out.println("duration:" + duration + " ms");
    }

}
