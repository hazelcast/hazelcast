/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MIN_RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RepairingTaskTest extends HazelcastTestSupport {

    @Test
    public void uses_configured_toleratedMissCount() throws Exception {
        int givenMaxToleratedMissCount = 123;
        Config config = new Config().setProperty(MAX_TOLERATED_MISS_COUNT.getName(), Integer.toString(givenMaxToleratedMissCount));
        RepairingTask repairingTask = newRepairingTask(config);

        assertEquals(givenMaxToleratedMissCount, repairingTask.maxToleratedMissCount);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throws_illegalArgumentException_when_toleratedMissCount_is_negative() throws Exception {
        Config config = new Config().setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "-1");
        newRepairingTask(config);
    }

    @Test
    public void uses_configured_reconciliationIntervalSeconds() throws Exception {
        int givenReconciliationIntervalSeconds = 91;
        Config config = new Config().setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), Integer.toString(givenReconciliationIntervalSeconds));
        RepairingTask repairingTask = newRepairingTask(config);

        assertEquals(givenReconciliationIntervalSeconds, NANOSECONDS.toSeconds(repairingTask.reconciliationIntervalNanos));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throws_illegalArgumentException_when_reconciliationIntervalSeconds_is_negative() throws Exception {
        Config config = new Config().setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), "-1");
        newRepairingTask(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throws_illegalArgumentException_when_reconciliationIntervalSeconds_is_not_zero_but_smaller_than_threshold_value() throws Exception {
        int thresholdValue = (int) MIN_RECONCILIATION_INTERVAL_SECONDS;
        Config config = new Config().setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), Integer.toString(getInt(1, thresholdValue)));
        newRepairingTask(config);
    }

    private RepairingTask newRepairingTask(Config config) {
        MetaDataFetcher metaDataFetcher = mock(MetaDataFetcher.class);
        ExecutionService executionService = mock(ExecutionService.class);
        MinimalPartitionService minimalPartitionService = mock(MinimalPartitionService.class);
        String uuid = UuidUtil.newUnsecureUUID().toString();
        ILogger logger = Logger.getLogger(RepairingTask.class);
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        return new RepairingTask(metaDataFetcher, executionService, minimalPartitionService, hazelcastProperties, uuid, logger);
    }
}
