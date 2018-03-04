/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

abstract class SlowOperationDetectorAbstractTest extends HazelcastTestSupport {

    private static final String DEFAULT_KEY = "key";
    private static final String DEFAULT_VALUE = "value";

    private List<SlowEntryProcessor> entryProcessors = new ArrayList<SlowEntryProcessor>();

    HazelcastInstance getSingleNodeCluster(int slowOperationThresholdMillis) {
        Config config = new Config();
        config.setProperty(GroupProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS.getName(),
                valueOf(slowOperationThresholdMillis));

        return createHazelcastInstance(config);
    }

    static IMap<String, String> getMapWithSingleElement(HazelcastInstance instance) {
        IMap<String, String> map = instance.getMap(randomMapName());
        map.put(DEFAULT_KEY, DEFAULT_VALUE);

        return map;
    }

    static void executeOperation(HazelcastInstance instance, Operation operation) {
        getOperationService(instance).execute(operation);
    }

    static void executeEntryProcessor(IMap<String, String> map, EntryProcessor<String, String> entryProcessor) {
        map.executeOnKey(DEFAULT_KEY, entryProcessor);
    }

    static void shutdownOperationService(HazelcastInstance instance) {
        if (instance == null) {
            return;
        }
        OperationServiceImpl operationService = (OperationServiceImpl) getOperationService(instance);
        operationService.shutdownInvocations();
        operationService.shutdownOperationExecutor();
    }

    static Collection<SlowOperationLog.Invocation> getInvocations(SlowOperationLog log) {
        Map<Integer, SlowOperationLog.Invocation> invocationMap = getFieldFromObject(log, "invocations");
        return invocationMap.values();
    }

    static int getDefaultPartitionId(HazelcastInstance instance) {
        return instance.getPartitionService().getPartition(DEFAULT_KEY).getPartitionId();
    }

    static JsonArray getSlowOperationLogsJsonArray(HazelcastInstance instance) {
        return getOperationStats(instance).get("slowOperations").asArray();
    }

    static JsonObject getOperationStats(HazelcastInstance instance) {
        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(instance));
        return timedMemberStateFactory.createTimedMemberState().getMemberState().getOperationStats().toJson();
    }

    static Collection<SlowOperationLog> getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(final HazelcastInstance instance,
                                                                                               final int expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
                assertNumberOfSlowOperationLogs(logs, expected);
            }
        });
        return getSlowOperationLogs(instance);
    }

    static Collection<SlowOperationLog> getSlowOperationLogs(HazelcastInstance instance) {
        InternalOperationService operationService = getOperationService(instance);
        SlowOperationDetector slowOperationDetector = getFieldFromObject(operationService, "slowOperationDetector");
        Map<Integer, SlowOperationLog> slowOperationLogs = getFieldFromObject(slowOperationDetector, "slowOperationLogs");

        return slowOperationLogs.values();
    }

    static void assertNumberOfSlowOperationLogs(Collection<SlowOperationLog> logs, int expected) {
        assertEqualsStringFormat("Expected %d slow operation logs, but was %d.", expected, logs.size());
    }

    static void assertTotalInvocations(SlowOperationLog log, int totalInvocations) {
        assertEqualsStringFormat("Expected %d total invocations, but was %d. Log: " + log.createDTO().toJson(),
                totalInvocations, log.totalInvocations.get());
    }

    static void assertEntryProcessorOperation(SlowOperationLog log) {
        String operation = log.operation;
        assertEqualsStringFormat("Expected operation %s, but was %s",
                "com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateOperation", operation);
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
        Integer duration = invocation.createDTO(0).durationMs;

        assertTrue(format("Duration of invocation should be >= %d, but was %d", min, duration), duration >= min);
        assertTrue(format("Duration of invocation should be <= %d, but was %d", max, duration), duration <= max);
    }

    SlowEntryProcessor getSlowEntryProcessor(int sleepSeconds) {
        SlowEntryProcessor entryProcessor = new SlowEntryProcessor(sleepSeconds);
        entryProcessors.add(entryProcessor);
        return entryProcessor;
    }

    void awaitSlowEntryProcessors() {
        for (SlowEntryProcessor slowEntryProcessor : entryProcessors) {
            slowEntryProcessor.await();
        }
    }

    @SuppressWarnings("unchecked")
    private static <E> E getFieldFromObject(Object object, String fieldName) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E) field.get(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class SlowEntryProcessor extends CountDownLatchHolder implements EntryProcessor<String, String> {

        static int GLOBAL_INSTANCE_COUNTER;

        final int instance = ++GLOBAL_INSTANCE_COUNTER;
        final int sleepSeconds;

        SlowEntryProcessor(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            sleepSeconds(sleepSeconds);
            done();
            return null;
        }

        @Override
        public EntryBackupProcessor<String, String> getBackupProcessor() {
            return null;
        }

        @Override
        public String toString() {
            return "SlowEntryProcessor{"
                    + "instance=" + instance
                    + ", sleepSeconds=" + sleepSeconds
                    + '}';
        }
    }

    static class SlowEntryProcessorChild extends SlowEntryProcessor {

        SlowEntryProcessorChild(int sleepSeconds) {
            super(sleepSeconds);
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            // not using sleepSeconds() here to have some variants in the stack traces
            try {
                TimeUnit.SECONDS.sleep(sleepSeconds);
            } catch (InterruptedException ignored) {
            }
            done();
            return null;
        }

        @Override
        public String toString() {
            return "SlowEntryProcessorChild{"
                    + "instance=" + instance
                    + ", sleepSeconds=" + sleepSeconds
                    + '}';
        }
    }

    static abstract class JoinableOperation extends Operation {

        private final CountDownLatch completedLatch = new CountDownLatch(1);

        void done() {
            completedLatch.countDown();
        }

        void join() {
            try {
                completedLatch.await();
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }

    static abstract class CountDownLatchHolder {

        private final CountDownLatch latch = new CountDownLatch(1);

        void done() {
            latch.countDown();
        }

        void await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }
}
