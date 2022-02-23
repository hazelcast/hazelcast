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

package com.hazelcast.query.impl.getters;

import com.fasterxml.jackson.core.JsonFactory;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.json.internal.JsonSchemaHelper;
import com.hazelcast.json.internal.JsonSchemaNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastSeconds;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * AbstractJsonGetter uses a lot of caching and guessing based on previously
 * encountered queries. Therefore, the first query and the subsequent
 * queries are likely to follow different execution path. This test is
 * to ensure that AbstractJsonGetter generates the same results for repeat
 * executions of the same query.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractJsonGetterTest {

    private AbstractJsonGetter getter = new JsonGetter();
    private JsonFactory factory = new JsonFactory();

    @Test
    public void testRepeatQueriesUseTheCachedContext() throws Exception {
        String jsonText = Json.object().add("at1", "val1").add("at2", "val2").toString();
        HazelcastJsonValue jsonValue = new HazelcastJsonValue(jsonText);
        JsonSchemaNode node = JsonSchemaHelper.createSchema(factory.createParser(jsonText));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));

        assertEquals(1, getter.getContextCacheSize());
    }

    @Test
    public void testDifferentQueriesCreateNewContexts() throws Exception {
        String jsonText = Json.object().add("at1", "val1").add("at2", "val2").toString();
        HazelcastJsonValue jsonValue = new HazelcastJsonValue(jsonText);
        JsonSchemaNode node = JsonSchemaHelper.createSchema(factory.createParser(jsonText));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val1", getter.getValue(jsonValue, "at1", node));
        assertEquals("val2", getter.getValue(jsonValue, "at2", node));
        assertEquals("val2", getter.getValue(jsonValue, "at2", node));
        assertEquals("val2", getter.getValue(jsonValue, "at2", node));
        assertEquals("val2", getter.getValue(jsonValue, "at2", node));

        assertEquals(2, getter.getContextCacheSize());
    }

    @Test
    public void testQueryObjectsWithDifferentPatterns() throws Exception {
        testRandomOrderObjectRepetitiveQuerying(100);
    }

    @Test
    public void testMultithreadedGetter() throws InterruptedException {
        int numberOfThreads = 5;
        Thread[] threads = new Thread[numberOfThreads];
        GetterRunner[] getterRunners = new GetterRunner[numberOfThreads];

        AtomicBoolean running = new AtomicBoolean();
        for (int i = 0; i < numberOfThreads; i++) {
            getterRunners[i] = new GetterRunner(running);
            threads[i] = new Thread(getterRunners[i]);
            threads[i].start();
        }
        running.set(true);
        sleepAtLeastSeconds(5);
        running.set(false);

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].join();
        }

        for (int i = 0; i < numberOfThreads; i++) {
            GetterRunner current = getterRunners[i];
            assertFalse(current.getStackTrace(), getterRunners[i].isFailed);
        }
    }

    private void testRandomOrderObjectRepetitiveQuerying(final int queryCount) throws Exception {
        for (int i = 0; i < queryCount; i++) {
            HazelcastJsonValue value = createJsonValueWithRandomStructure(
                    new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10"},
                    new String[]{"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"});
            JsonSchemaNode schema = createMetadata(value);

            assertEquals("v1", getter.getValue(value, "a1", schema));
            assertEquals("v2", getter.getValue(value, "a2", schema));
            assertEquals("v3", getter.getValue(value, "a3", schema));
            assertEquals("v4", getter.getValue(value, "a4", schema));
            assertEquals("v5", getter.getValue(value, "a5", schema));
            assertEquals("v6", getter.getValue(value, "a6", schema));
            assertEquals("v7", getter.getValue(value, "a7", schema));
            assertEquals("v8", getter.getValue(value, "a8", schema));
            assertEquals("v9", getter.getValue(value, "a9", schema));
            assertEquals("v10", getter.getValue(value, "a10", schema));
        }
        assertTrue(getter.getContextCacheSize() <= 10);
    }

    private JsonSchemaNode createMetadata(HazelcastJsonValue value) throws IOException {
        return JsonSchemaHelper.createSchema(factory.createParser(value.toString()));
    }

    private class GetterRunner implements Runnable {

        private AtomicBoolean running;
        private boolean isFailed;
        private Throwable exception;

        GetterRunner(AtomicBoolean running) {
            this.running = running;
        }

        public boolean isFailed() {
            return isFailed;
        }

        public Throwable getThrowable() {
            return exception;
        }

        public String getStackTrace() {
            if (exception != null) {
                return Arrays.toString(exception.getStackTrace());
            } else {
                return null;
            }
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    testRandomOrderObjectRepetitiveQuerying(10);
                } catch (Throwable e) {
                    exception = e;
                    isFailed = true;
                    running.set(false);
                }
            }
        }
    }

    /**
     * Creates a one level json object from given names and values. Each
     * value is associated with the respective name in given order. However,
     * the order of name-value pairs within object are random
     * @param names
     * @param values
     * @return
     */
    private HazelcastJsonValue createJsonValueWithRandomStructure(String[] names, String[] values) {
        Random random = new Random();
        for (int i = names.length - 1; i > 0; i--) {
            int swapIndex = random.nextInt(i + 1);
            String tempName = names[i];
            names[i] = names[swapIndex];
            names[swapIndex] = tempName;

            String tempValue = values[i];
            values[i] = values[swapIndex];
            values[swapIndex] = tempValue;
        }

        JsonObject object = Json.object();
        for (int i = 0; i < names.length; i++) {
            object.add(names[i], values[i]);
        }
        return new HazelcastJsonValue(object.toString());
    }
}
