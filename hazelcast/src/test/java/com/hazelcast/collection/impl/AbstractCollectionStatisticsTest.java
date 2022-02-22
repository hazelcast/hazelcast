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

package com.hazelcast.collection.impl;

import com.hazelcast.collection.LocalCollectionStats;
import com.hazelcast.test.HazelcastTestSupport;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCollectionStatisticsTest extends HazelcastTestSupport {

    protected LocalCollectionStats localCollectionStats;

    protected long previousAccessTime;
    protected long previousUpdateTime;

    protected void assertNewLastAccessTime() {
        assertTrueEventually(() -> {
            long lastAccessTime = localCollectionStats.getLastAccessTime();
            assertTrue(format("Expected the lastAccessTime %d to be higher than the previousAccessTime %d (diff: %d ms)",
                    lastAccessTime, previousAccessTime, lastAccessTime - previousAccessTime),
                    lastAccessTime > previousAccessTime);
            previousAccessTime = lastAccessTime;
        });
    }

    protected void assertSameLastUpdateTime() {
        long lastUpdateTime = localCollectionStats.getLastUpdateTime();
        assertEqualsStringFormat("Expected the lastUpdateTime to be %d, but was %d", previousUpdateTime, lastUpdateTime);
        previousUpdateTime = lastUpdateTime;
    }

    protected void assertNewLastUpdateTime() {
        assertTrueEventually(() -> {
            long lastUpdateTime = localCollectionStats.getLastUpdateTime();
            assertTrue(format("Expected the lastUpdateTime %d to be higher than the previousAccessTime %d (diff: %d ms)",
                    lastUpdateTime, previousUpdateTime, lastUpdateTime - previousUpdateTime),
                    lastUpdateTime > previousUpdateTime);
            previousUpdateTime = lastUpdateTime;
        });
    }
}
