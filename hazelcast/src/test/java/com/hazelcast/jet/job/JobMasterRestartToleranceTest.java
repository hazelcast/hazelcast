/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.job;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@Category({SlowTest.class, ParallelJVMTest.class})
public class JobMasterRestartToleranceTest extends SimpleTestInClusterSupport {

    private static final int INSTANCE_NUMBER = 3;
    private static final String JOB_NAME = "job";

    @BeforeClass
    public static void setup() {
        initializeWithClient(INSTANCE_NUMBER, null, null);
    }

    @Test
    public void memberGetJobTolerance() throws InterruptedException {
        var nonMaster = instances()[1];
        var stop = new AtomicBoolean(false);
        var result = startAsyncProcess(stop, () -> nonMaster.getJet().getJob(JOB_NAME));
        restart(0); // restart master
        sleepAndStop(stop, 3);
        assertThat(result).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
    }

    private void restart(int i) {
        instances()[i].shutdown();
        instances()[i] = factory().newHazelcastInstance(smallInstanceConfig());
        sleepSeconds(1);
    }

    private CompletableFuture<Void> startAsyncProcess(AtomicBoolean stop, Runnable runnable) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        var f =  CompletableFuture.runAsync(() -> {
            while (!stop.get()) {
                try {
                    latch.countDown();
                    runnable.run();
                } catch (Exception e) {
                    stop.set(true);
                    throw new CompletionException(e);
                }
            }
        });
        latch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        return f;
    }
}
