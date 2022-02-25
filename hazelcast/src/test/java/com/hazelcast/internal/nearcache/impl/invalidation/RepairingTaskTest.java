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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MIN_RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RepairingTaskTest extends HazelcastTestSupport {

    @Test
    public void whenToleratedMissCountIsConfigured_thenItShouldBeUsed() {
        int maxToleratedMissCount = 123;
        Config config = getConfigWithMaxToleratedMissCount(maxToleratedMissCount);
        RepairingTask repairingTask = newRepairingTask(config);

        assertEquals(maxToleratedMissCount, repairingTask.maxToleratedMissCount);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenToleratedMissCountIsNegative_thenThrowException() {
        Config config = getConfigWithMaxToleratedMissCount(-1);
        newRepairingTask(config);
    }

    @Test
    public void whenReconciliationIntervalSecondsIsConfigured_thenItShouldBeUsed() {
        int reconciliationIntervalSeconds = 91;
        Config config = getConfigWithReconciliationInterval(reconciliationIntervalSeconds);
        RepairingTask repairingTask = newRepairingTask(config);

        assertEquals(reconciliationIntervalSeconds, NANOSECONDS.toSeconds(repairingTask.reconciliationIntervalNanos));
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenReconciliationIntervalSecondsIsNegative_thenThrowException() {
        Config config = getConfigWithReconciliationInterval(-1);
        newRepairingTask(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenReconciliationIntervalSecondsIsNotZeroButSmallerThanThresholdValue_thenThrowException() {
        int thresholdValue = Integer.parseInt(MIN_RECONCILIATION_INTERVAL_SECONDS.getDefaultValue());
        Config config = getConfigWithReconciliationInterval(getInt(1, thresholdValue));
        newRepairingTask(config);
    }

    private static Config getConfigWithMaxToleratedMissCount(int maxToleratedMissCount) {
        return new Config()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), Integer.toString(maxToleratedMissCount));
    }

    private static Config getConfigWithReconciliationInterval(int reconciliationIntervalSeconds) {
        return new Config()
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), Integer.toString(reconciliationIntervalSeconds));
    }

    private static RepairingTask newRepairingTask(Config config) {
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher = mock(InvalidationMetaDataFetcher.class);
        ExecutionService executionService = mock(ExecutionService.class);
        SerializationService serializationService = mock(SerializationService.class);
        MinimalPartitionService minimalPartitionService = mock(MinimalPartitionService.class);
        UUID uuid = UuidUtil.newUnsecureUUID();
        ILogger logger = Logger.getLogger(RepairingTask.class);

        return new RepairingTask(hazelcastProperties, invalidationMetaDataFetcher, executionService.getGlobalTaskScheduler(),
                serializationService, minimalPartitionService, uuid, logger);
    }
}
