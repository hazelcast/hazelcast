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
package com.hazelcast.jet.kinesis.impl.source;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.kinesis.impl.source.HashRange.range;
import static com.hazelcast.jet.kinesis.impl.source.ShardTracker.EXPIRATION_MS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ShardTrackerTest {

    private static final HashRange[] RANGE_PARTITIONS = {range(0, 1000L), range(1000L, 2000L), range(2000L, 3000L)};

    private static final Shard SHARD0 = shard("shard0", 0L, 500L);
    private static final Shard SHARD1 = shard("shard1", 500L, 1000L);
    private static final Shard SHARD2 = shard("shard2", 1000L, 1500L);
    private static final Shard SHARD3 = shard("shard3", 1500L, 2000L);
    private static final Shard SHARD4 = shard("shard4", 2000L, 2500L);
    private static final Shard SHARD5 = shard("shard5", 2500L, 3000L);

    private static final long MINUTE_MS = MINUTES.toMillis(1);

    private ShardTracker tracker = new ShardTracker(RANGE_PARTITIONS);

    @Test
    public void expiration() {
        addUndetected(SHARD2, 0);
        addUndetected(SHARD4, 0);

        tracker.markDetections(Collections.singleton(SHARD0), MINUTE_MS);
        tracker.markDetections(Collections.singleton(SHARD1), MINUTE_MS);
        tracker.markDetections(Collections.singleton(SHARD3), MINUTE_MS);

        tracker.markDetections(Collections.singleton(SHARD0), 2 * MINUTE_MS);
        tracker.markDetections(Collections.singleton(SHARD5), 2 * MINUTE_MS);

        tracker.markDetections(Collections.singleton(SHARD0), 3 * MINUTE_MS);
        tracker.markDetections(Collections.singleton(SHARD2), 3 * MINUTE_MS);

        assertExpired(0);
        assertExpired(EXPIRATION_MS);
        assertExpired(EXPIRATION_MS + 1, SHARD4, 2);
        assertExpired(EXPIRATION_MS + 1);
        assertExpired(EXPIRATION_MS + MINUTE_MS);
        assertExpired(EXPIRATION_MS + MINUTE_MS + 1, SHARD1, 0, SHARD3, 1);
        assertExpired(EXPIRATION_MS + 2 * MINUTE_MS);
        assertExpired(EXPIRATION_MS + 2 * MINUTE_MS + 1, SHARD5, 2);
        assertExpired(EXPIRATION_MS + 3 * MINUTE_MS);
        assertExpired(EXPIRATION_MS + 3 * MINUTE_MS + 1, SHARD0, 0, SHARD2, 1);
    }

    @Test
    public void detection() {
        addUndetected(SHARD2, 0);
        addUndetected(SHARD4, 0);

        assertNew(set(SHARD0, SHARD2, SHARD5), SHARD0, 0, SHARD2, 1, SHARD5, 2);
        assertNew(set(SHARD0, SHARD2, SHARD5));
        assertNew(set(SHARD1, SHARD3, SHARD4), SHARD1, 0, SHARD3, 1, SHARD4, 2);
        assertNew(set(SHARD1, SHARD3, SHARD4));
    }

    private void assertNew(Set<Shard> shards, Object... expectedValues) {
        assertEquals(expectedNew(expectedValues), tracker.markDetections(shards, 0));
    }

    private void assertExpired(long timeMs, Object... expectedValues) {
        assertEquals(expectedExpired(expectedValues), tracker.removeExpiredShards(timeMs));
    }

    private void addUndetected(Shard shard, long timeMs) {
        tracker.addUndetected(shard.getShardId(), new BigInteger(shard.getHashKeyRange().getStartingHashKey()), timeMs);
    }

    private static Set<Shard> set(Shard... shards) {
        return new HashSet<>(Arrays.asList(shards));
    }

    private static Shard shard(String id, long startHashKey, long endHashKey) {
        HashKeyRange hashKeyRange = new HashKeyRange()
                .withStartingHashKey(Long.toString(startHashKey))
                .withEndingHashKey(Long.toString(endHashKey));
        return new Shard().withShardId(id).withHashKeyRange(hashKeyRange);
    }

    private static Map<Shard, Integer> expectedNew(Object... values) {
        Map<Shard, Integer> retMap = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            Shard shard = (Shard) values[i];
            Integer owner = (Integer) values[i + 1];
            retMap.put(shard, owner);
        }
        return retMap;
    }

    private static Map<String, Integer> expectedExpired(Object... values) {
        Map<String, Integer> retMap = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            Shard shard = (Shard) values[i];
            Integer owner = (Integer) values[i + 1];
            retMap.put(shard.getShardId(), owner);
        }
        return retMap;
    }
}
