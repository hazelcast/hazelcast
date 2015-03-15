/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.slowoperationdetector;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.impl.BasicOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class SlowOperationDetector_purgeTest extends SlowOperationDetectorAbstractTest {

    @Test
    public void testPurging_Invocation() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "1000");
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS, "3");
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS, "1");

        HazelcastInstance instance = getSingleNodeCluster(config);
        IMap<String, String> map = getMapWithSingleElement(instance);

        // all of these entry processors are executed after each other, not in parallel
        // so only the last one will survive the purging
        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(new SlowEntryProcessor(2));
        }
        map.executeOnEntries(new SlowEntryProcessor(3));
        map.executeOnEntries(new SlowEntryProcessor(2));

        BasicOperationService operationService = (BasicOperationService) getInternalOperationService(instance);
        operationService.shutdown();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 4);
        assertEntryProcessorOperation(firstLog);
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");

        Collection<SlowOperationLog.Invocation> invocations = getInvocations(firstLog);
        assertEqualsStringFormat("Expected %d invocations, but was %d", 2, invocations.size());
        for (SlowOperationLog.Invocation invocation : invocations) {
            assertInvocationDurationBetween(invocation, 1000, 3500);
        }
    }

    @Test
    public void testPurging_SlowOperationLog() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "1000");
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS, "2");
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS, "1");

        HazelcastInstance instance = getSingleNodeCluster(config);
        IMap<String, String> map = getMapWithSingleElement(instance);

        // all of these entry processors are executed after each other, not in parallel
        // so none of them will survive the purging
        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(new SlowEntryProcessor(2));
        }
        sleepSeconds(3);

        BasicOperationService operationService = (BasicOperationService) getInternalOperationService(instance);
        operationService.shutdown();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 0);
    }
}
