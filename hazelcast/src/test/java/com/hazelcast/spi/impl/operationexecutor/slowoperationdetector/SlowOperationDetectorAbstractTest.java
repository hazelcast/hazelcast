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

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.lang.reflect.Field;
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
        final InternalOperationService operationService = getInternalOperationService(instance);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(operationService.getRunningOperationsCount() == 0);
            }
        });
    }

    static void shutdownOperationService(HazelcastInstance instance) {
        OperationServiceImpl operationService = (OperationServiceImpl) getInternalOperationService(instance);
        operationService.shutdown();
    }

    static Collection<SlowOperationLog> getSlowOperationLogs(HazelcastInstance instance) {
        InternalOperationService operationService = getInternalOperationService(instance);
        SlowOperationDetector slowOperationDetector = getFieldFromObject(operationService, "slowOperationDetector");
        Map<Integer, SlowOperationLog> slowOperationLogs = getFieldFromObject(slowOperationDetector, "slowOperationLogs");

        return slowOperationLogs.values();
    }

    static Collection<SlowOperationLog.Invocation> getInvocations(SlowOperationLog log) {
        Map<Integer, SlowOperationLog.Invocation> invocationMap = getFieldFromObject(log, "invocations");
        return invocationMap.values();
    }

    static InternalOperationService getInternalOperationService(HazelcastInstance instance) {
        return TestUtil.getNode(instance).nodeEngine.getOperationService();
    }

    static int getDefaultPartitionId(HazelcastInstance instance) {
        return instance.getPartitionService().getPartition(DEFAULT_KEY).getPartitionId();
    }

    static JsonArray getSlowOperationLogsJsonArray(HazelcastInstance instance) {
        return getOperationStats(instance).get("slowOperations").asArray();
    }

    private static JsonObject getOperationStats(HazelcastInstance instance) {
        return getTimedMemberState(instance).getMemberState().getOperationStats().toJson();
    }

    static TimedMemberState getTimedMemberState(HazelcastInstance instance) {
        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(instance));
        return timedMemberStateFactory.createTimedMemberState();
    }

    static void assertNumberOfSlowOperationLogs(Collection<SlowOperationLog> logs, int expected) {
        assertEqualsStringFormat("Expected %d slow operation logs, but was %d", expected, logs.size());
    }

    static void assertTotalInvocations(SlowOperationLog log, int totalInvocations) {
        assertEqualsStringFormat("Expected %d total invocations, but was %d", totalInvocations, log.totalInvocations);
    }

    static void assertEntryProcessorOperation(SlowOperationLog log) {
        String operation = log.operation;
        assertEqualsStringFormat("Expected operation %s, but was %s", "PartitionWideEntryWithPredicateOperation{}", operation);
    }

    static void assertOperationContainsClassName(SlowOperationLog log, String className) {
        String operation = log.operation;
        assertTrue(format("Expected operation to contain '%s'%n%s", className, operation), operation.contains("$" + className));
    }

    static void assertStackTraceContainsClassName(SlowOperationLog log, String className) {
        String stackTrace = log.stackTrace;
        assertTrue(format("Expected stacktrace to contain className '%s'%n%s", className, stackTrace),
                stackTrace.contains("$" + className + "."));
    }

    static void assertStackTraceNotContainsClassName(SlowOperationLog log, String className) {
        String stackTrace = log.stackTrace;
        assertFalse(format("Expected stacktrace to not contain className '%s'%n%s", className, stackTrace),
                stackTrace.contains(className));
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

    static void assertInvocationDurationBetween(SlowOperationLog.Invocation invocation, int min, int max) {
        Integer duration = invocation.createDTO().durationMs;

        assertTrue(format("Duration of invocation should be >= %d, but was %d", min, duration), duration >= min);
        assertTrue(format("Duration of invocation should be <= %d, but was %d", max, duration), duration <= max);
    }

    @SuppressWarnings("unchecked")
    static <E> E getFieldFromObject(Object object, String fieldName) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E) field.get(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
