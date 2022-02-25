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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JetTestSupport.assertJobRunningEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public final class SinkStressTestUtil {

    private static final int TEST_TIMEOUT_SECONDS = 120;

    private SinkStressTestUtil() { }

    public static void test_withRestarts(
            @Nonnull HazelcastInstance instance,
            @Nonnull ILogger logger,
            @Nonnull Sink<Integer> sink,
            boolean graceful,
            boolean exactlyOnce,
            @Nonnull SupplierEx<List<Integer>> actualItemsSupplier
    ) {
        int numItems = 1000;
        Pipeline p = Pipeline.create();
        p.readFrom(SourceBuilder.stream("src", procCtx -> new int[]{procCtx.globalProcessorIndex() == 0 ? 0 : Integer.MAX_VALUE})
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (ctx[0] < numItems) {
                        buf.add(ctx[0]++);
                        sleepMillis(5);
                    }
                })
                .distributed(1)
                .createSnapshotFn(ctx -> ctx[0] < Integer.MAX_VALUE ? ctx[0] : null)
                .restoreSnapshotFn((ctx, state) -> ctx[0] = ctx[0] != Integer.MAX_VALUE ? state.get(0) : Integer.MAX_VALUE)
                .build())
         .withoutTimestamps()
         .peek()
         .writeTo(sink);

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(exactlyOnce ? EXACTLY_ONCE : AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance.getJet().newJob(p, config);

        long endTime = System.nanoTime() + SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
        int lastCount = 0;
        String expectedRows = IntStream.range(0, numItems)
                                       .mapToObj(i -> i + (exactlyOnce ? "=1" : ""))
                                       .collect(joining("\n"));
        // We'll restart once, then restart again after a short sleep (possibly during initialization),
        // and then assert some output so that the test isn't constantly restarting without any progress
        Long lastExecutionId = null;
        for (;;) {
            lastExecutionId = assertJobRunningEventually(instance, job, lastExecutionId);
            job.restart(graceful);
            lastExecutionId = assertJobRunningEventually(instance, job, lastExecutionId);
            sleepMillis(ThreadLocalRandom.current().nextInt(400));
            job.restart(graceful);
            try {
                List<Integer> actualItems;
                Set<Integer> distinctActualItems;
                do {
                    actualItems = actualItemsSupplier.get();
                    distinctActualItems = new HashSet<>(actualItems);
                } while (distinctActualItems.size() < Math.min(numItems, 100 + lastCount)
                        && System.nanoTime() < endTime);
                lastCount = distinctActualItems.size();
                logger.info("number of committed items in the sink so far: " + lastCount);
                if (exactlyOnce) {
                    String actualItemsStr = actualItems.stream()
                            .collect(groupingBy(identity(), TreeMap::new, counting()))
                            .entrySet().stream()
                            .map(Object::toString)
                            .collect(joining("\n"));
                    assertEquals(expectedRows, actualItemsStr);
                } else {
                    assertEquals(expectedRows, distinctActualItems.stream().map(Objects::toString).collect(joining("\n")));
                }
                // if content matches, break the loop. Otherwise restart and try again
                break;
            } catch (AssertionError e) {
                if (System.nanoTime() >= endTime) {
                    throw e;
                }
            }
        }
    }
}
