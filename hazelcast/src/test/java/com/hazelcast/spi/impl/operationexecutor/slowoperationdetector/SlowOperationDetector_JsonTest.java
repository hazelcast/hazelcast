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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class SlowOperationDetector_JsonTest extends SlowOperationDetectorAbstractTest {

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
        log.getOrCreateInvocation(id, durationNanos, nowNanos, nowMillis);

        JsonObject json = log.toJson();
        SlowOperationLog actual = new SlowOperationLog(json);

        System.out.println(json);

        assertEqualsStringFormat("Expected stack trace '%s', but was '%s'", stackTrace, actual.getStackTrace());
        assertEqualsStringFormat("Expected operation '%s', but was '%s'", operation, getFieldFromObject(actual, "operation"));
        assertEqualsStringFormat("Expected totalInvocations '%d', but was '%d'", 1, getFieldFromObject(actual,
                "totalInvocations"));

        ConcurrentHashMap<Integer, SlowOperationLog.Invocation> invocations = getFieldFromObject(actual, "invocations");
        SlowOperationLog.Invocation invocation = invocations.values().iterator().next();

        assertEqualsStringFormat("Expected id '%d', but was '%d'", id, getFieldFromObject(invocation, "id"));
        assertEqualsStringFormat("Expected startedAt '%d', but was '%d'", nowMillis - durationMs, getFieldFromObject(invocation,
                "startedAt"));
        assertEqualsStringFormat("Expected durationMs '%d', but was '%d'", durationMs, getFieldFromObject(invocation,
                "durationMs"));
    }

    @Test
    public void testJSON_SlowEntryProcessor() {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(new SlowEntryProcessor(2));
        }
        map.executeOnEntries(new SlowEntryProcessor(3));
        map.executeOnEntries(new SlowEntryProcessor(2));
        waitForAllOperationsToComplete(instance);

        //System.out.println(getTimedMemberState(instance).toJson());

        JsonObject firstLogJsonObject = getSlowOperationLogsJsonArray(instance).get(0).asObject();
        assertJSONContainsClassName(firstLogJsonObject, "SlowEntryProcessor");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4, firstLogJsonObject.get("invocations").asArray().size());
    }

    @Test
    public void testJSON_multipleEntryProcessorClasses() throws InterruptedException {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(new SlowEntryProcessor(2));
        }
        map.executeOnEntries(new SlowEntryProcessorChild(2));
        map.executeOnEntries(new SlowEntryProcessor(4));
        waitForAllOperationsToComplete(instance);

        //System.out.println(getTimedMemberState(instance).toJson());

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
