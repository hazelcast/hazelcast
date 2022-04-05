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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.test.HazelcastTestSupport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public final class FlakeIdConcurrencyTestUtil {

    public static final int NUM_THREADS = 4;
    public static final int IDS_IN_THREAD = 100000;

    private FlakeIdConcurrencyTestUtil() {
    }

    public static Set<Long> concurrentlyGenerateIds(final Supplier<Long> generator) throws Exception {
        List<Future<Set<Long>>> futures = new ArrayList<>();
        final CountDownLatch startLatch = new CountDownLatch(1);
        for (int i = 0; i < NUM_THREADS; i++) {
            futures.add(HazelcastTestSupport.spawn(() -> {
                Set<Long> localIds = new HashSet<>(IDS_IN_THREAD);
                startLatch.await();
                for (int i1 = 0; i1 < IDS_IN_THREAD; i1++) {
                    localIds.add(generator.get());
                }
                return localIds;
            }));
        }

        startLatch.countDown();
        Set<Long> ids = new HashSet<>();
        for (Future<Set<Long>> f : futures) {
            ids.addAll(f.get());
        }

        // if there were duplicate IDs generated, there will be less items in the set than expected
        assertEquals(NUM_THREADS * IDS_IN_THREAD, ids.size());
        return ids;
    }
}
