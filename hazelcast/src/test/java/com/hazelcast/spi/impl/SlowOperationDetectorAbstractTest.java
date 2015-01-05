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

package com.hazelcast.spi.impl;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.management.TimedMemberStateFactory;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

abstract class SlowOperationDetectorAbstractTest extends HazelcastTestSupport {

    private static final String DEFAULT_KEY = "key";
    private static final String DEFAULT_VALUE = "value";

    HazelcastInstance getSingleNodeCluster(int slowOperationThresholdMillis) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS,
                String.valueOf(slowOperationThresholdMillis));

        return getSingleNodeCluster(config);
    }

    HazelcastInstance getSingleNodeCluster(Config config) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("slowOperation*");
        config.addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        return factory.newHazelcastInstance(config);
    }

    static IMap<String, String> getMapWithSingleElement(HazelcastInstance instance) {
        IMap<String, String> map = instance.getMap("slowOperation" + randomMapName());
        map.put(DEFAULT_KEY, DEFAULT_VALUE);

        return map;
    }

    static void executeOperation(HazelcastInstance instance, Operation operation) {
        getInternalOperationService(instance).executeOperation(operation);
    }

    static void executeEntryProcessor(IMap<String, String> map, EntryProcessor<String, String> entryProcessor) {
        map.executeOnKey(DEFAULT_KEY, entryProcessor);
    }

    static void waitForAllOperationsToComplete(final HazelcastInstance instance) {
        sleepSeconds(1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(((BasicOperationService) getInternalOperationService(instance)).getRunningOperationsCount() == 0);
            }
        });
    }

    static Collection<SlowOperationLog> getSlowOperationLogs(HazelcastInstance instance) {
        return getInternalOperationService(instance).getSlowOperationLogs();
    }

    static InternalOperationService getInternalOperationService(HazelcastInstance instance) {
        return ((InternalOperationService) TestUtil.getNode(instance).nodeEngine.getOperationService());
    }

    static int getDefaultPartitionId(HazelcastInstance instance) {
        return instance.getPartitionService().getPartition(DEFAULT_KEY).getPartitionId();
    }

    static JsonArray getSlowOperationLogsJsonArray(HazelcastInstance instance) {
        return getOperationStats(instance).get("slowOperationLogs").asArray();
    }

    private static JsonObject getOperationStats(HazelcastInstance instance) {
        return getTimedMemberState(instance).getMemberState().getOperationStats().toJson();
    }

    private static TimedMemberState getTimedMemberState(HazelcastInstance instance) {
        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(instance));
        return timedMemberStateFactory.createTimedMemberState();
    }

    static void assertNumberOfSlowOperationLogs(Collection<SlowOperationLog> logs, int expected) {
        assertEqualsStringFormat("Expected %d slow operation logs, but was %d", expected, logs.size());
    }

    static void assertTotalInvocations(SlowOperationLog log, int totalInvocations) {
        assertEqualsStringFormat("Expected %d total invocations, but was %d", totalInvocations, log.getTotalInvocations());
    }

    static void assertEntryProcessorOperation(SlowOperationLog log) {
        assertEqualsStringFormat("Expected operation %s, but was %s", "PartitionWideEntryWithPredicateOperation{}",
                log.getOperation());
    }

    static void assertOperationContainsClassName(SlowOperationLog log, String className) {
        assertTrue(format("Expected operation to contain '%s'%n%s", className, log.getOperation()),
                log.getOperation().contains("$" + className));
    }

    static void assertStackTraceContainsClassName(SlowOperationLog log, String className) {
        assertTrue(format("Expected stacktrace to contain className '%s'%n%s", className, log.getStackTrace()),
                log.getStackTrace().contains("$" + className + "."));
    }

    static void assertStackTraceNotContainsClassName(SlowOperationLog log, String className) {
        assertFalse(format("Expected stacktrace to not contain className '%s'%n%s", className, log.getStackTrace()),
                log.getStackTrace().contains(className));
    }

    static void assertJSONContainsClassName(JsonObject jsonObject, String className) {
        String stackTrace = jsonObject.get("stackTrace").toString();
        assertTrue(format("JSON for Management Center should contain stackTrace with class name '%s'%n%s", className, stackTrace),
                stackTrace.contains("$" + className + "."));
    }

    static void assertJSONContainsClassNameJustOnce(JsonObject jsonObject1, JsonObject jsonObject2, String className) {
        boolean firstClassFound = jsonObject1.get("stackTrace").toString().contains("$" + className + ".");
        boolean secondClassFound = jsonObject2.get("stackTrace").toString().contains("$" + className + ".");
        assertTrue(format("JSON for Management Center should contain stackTrace with class name '%s' exactly once", className),
                firstClassFound ^ secondClassFound);
    }

    static void assertDurationBetween(long duration, int min, int max) {
        assertTrue(format("Duration of invocation should be >= %d, but was %d", min, duration), duration >= min);
        assertTrue(format("Duration of invocation should be <= %d, but was %d", max, duration), duration <= max);
    }

    static class SlowEntryProcessor implements EntryProcessor<String, String> {

        final int sleepSeconds;

        SlowEntryProcessor(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            sleepSeconds(sleepSeconds);
            return null;
        }

        @Override
        public EntryBackupProcessor<String, String> getBackupProcessor() {
            return null;
        }
    }

    static class SlowEntryProcessorChild extends SlowEntryProcessor {

        SlowEntryProcessorChild(int sleepSeconds) {
            super(sleepSeconds);
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            try {
                TimeUnit.SECONDS.sleep(sleepSeconds);
            } catch (InterruptedException ignored) {
            }
            return null;
        }
    }
}
