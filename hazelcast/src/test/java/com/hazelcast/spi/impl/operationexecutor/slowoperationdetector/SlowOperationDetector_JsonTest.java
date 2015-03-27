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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.management.dto.SlowOperationInvocationDTO;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class SlowOperationDetector_JsonTest extends SlowOperationDetectorAbstractTest {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private ILogger logger;

    @Before
    public void setup() {
        instance = getSingleNodeCluster(1000);
        map = getMapWithSingleElement(instance);
        logger = instance.getLoggingService().getLogger("SlowOperationDetector_JsonTest");
    }

    @After
    public void teardown() {
        shutdownOperationService(instance);
        shutdownNodeFactory();
    }

    @Test
    public void testJSON() throws InterruptedException {
        String stackTrace = "stackTrace";
        String operation = "fakeOperation";
        int id = 5;
        int durationMs = 4444;
        long nowMillis = System.currentTimeMillis();
        long nowNanos = System.nanoTime();
        long durationNanos = TimeUnit.MILLISECONDS.toNanos(durationMs);

        SlowOperationLog log = new SlowOperationLog(stackTrace, operation);
        log.totalInvocations.incrementAndGet();
        log.getOrCreate(id, durationNanos, nowNanos, nowMillis);

        JsonObject json = log.createDTO().toJson();
        logger.finest(json.toString());

        SlowOperationDTO slowOperationDTO = new SlowOperationDTO();
        slowOperationDTO.fromJson(json);
        assertEqualsStringFormat("Expected stack trace '%s', but was '%s'", stackTrace, slowOperationDTO.stackTrace);
        assertEqualsStringFormat("Expected operation '%s', but was '%s'", operation, slowOperationDTO.operation);
        assertEqualsStringFormat("Expected totalInvocations '%d', but was '%d'", 1, slowOperationDTO.totalInvocations);
        assertEqualsStringFormat("Expected invocations.size() '%d', but was '%d'", 1, slowOperationDTO.invocations.size());

        SlowOperationInvocationDTO invocationDTO = slowOperationDTO.invocations.get(0);
        assertEqualsStringFormat("Expected id '%d', but was '%d'", id, invocationDTO.id);
        assertEqualsStringFormat("Expected startedAt '%d', but was '%d'", nowMillis - durationMs, invocationDTO.startedAt);
        assertEqualsStringFormat("Expected durationMs '%d', but was '%d'", durationMs, invocationDTO.durationMs);
    }

    @Test
    public void testJSON_SlowEntryProcessor() {
        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(new SlowEntryProcessor(2));
        }
        map.executeOnEntries(new SlowEntryProcessor(3));
        map.executeOnEntries(new SlowEntryProcessor(2));
        waitForAllOperationsToComplete(instance);

        logger.finest(getOperationStats(instance).toString());

        JsonObject firstLogJsonObject = getSlowOperationLogsJsonArray(instance).get(0).asObject();
        assertJSONContainsClassName(firstLogJsonObject, "SlowEntryProcessor");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4, firstLogJsonObject.get("invocations").asArray().size());
    }

    @Test
    public void testJSON_multipleEntryProcessorClasses() throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(new SlowEntryProcessor(2));
        }
        map.executeOnEntries(new SlowEntryProcessorChild(2));
        map.executeOnEntries(new SlowEntryProcessor(4));
        waitForAllOperationsToComplete(instance);

        logger.finest(getOperationStats(instance).toString());

        JsonArray slowOperationLogsJsonArray = getSlowOperationLogsJsonArray(instance);
        JsonObject firstLogJsonObject = slowOperationLogsJsonArray.get(0).asObject();
        JsonObject secondLogJsonObject = slowOperationLogsJsonArray.get(1).asObject();

        assertJSONContainsClassNameJustOnce(firstLogJsonObject, secondLogJsonObject, "SlowEntryProcessor");
        assertJSONContainsClassNameJustOnce(firstLogJsonObject, secondLogJsonObject, "SlowEntryProcessorChild");

        int firstSize = firstLogJsonObject.get("invocations").asArray().size();
        int secondSize = secondLogJsonObject.get("invocations").asArray().size();
        assertTrue(format("Expected to find 1 and 3 invocations in logs, but was %d and %d", firstSize, secondSize),
                (firstSize == 1 ^ secondSize == 1) && (firstSize == 3 ^ secondSize == 3));
    }
}
