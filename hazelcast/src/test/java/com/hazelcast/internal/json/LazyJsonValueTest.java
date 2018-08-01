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

package com.hazelcast.internal.json;

import com.hazelcast.json.Json;
import com.hazelcast.json.JsonObject;
import com.hazelcast.json.JsonValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelTest.class, QuickTest.class})
public class LazyJsonValueTest {

    @Test
    public void testDoesNotParseUntilTouched() {
        LazyJsonValue value = new LazyJsonValue("5");
        assertFalse(value.isParsed());

        value.asInt();
        assertTrue(value.isParsed());
    }

    @Test
    public void testEquals() {
        LazyJsonValue lazyObject = new LazyJsonValue("{ \"age\": 4 }");
        LazyJsonValue lazyObjectPadded = new LazyJsonValue("{ \"age\":      4 }");
        LazyJsonValue ageDifferent = new LazyJsonValue("{ \"age\": 5 }");
        JsonValue notLazyObject = Json.object().add("age", 4);

        assertEquals(lazyObject, lazyObjectPadded);
        assertEquals(lazyObjectPadded, lazyObject);
        assertEquals(lazyObject, notLazyObject);
        assertEquals(notLazyObject, lazyObject);
        assertNotEquals(lazyObject, ageDifferent);
        assertNotEquals(ageDifferent, lazyObject);


        LazyJsonValue lazyArray = new LazyJsonValue("[4, 5]");
        LazyJsonValue lazyArrayPadded = new LazyJsonValue("[4,       5]");
        LazyJsonValue lazyArrayDifferent = new LazyJsonValue("[4, 5, 6]");
        JsonValue notLazyArray = Json.array().add(4).add(5);

        assertEquals(lazyArray, lazyArrayPadded);
        assertEquals(lazyArrayPadded, lazyArray);
        assertEquals(lazyArray, notLazyArray);
        assertEquals(notLazyArray, lazyArray);
        assertNotEquals(lazyArray, lazyArrayDifferent);
        assertNotEquals(lazyArrayDifferent, lazyArray);
    }

    @Test
    public void testAsObjectReturnsTheSameUnderlyingObject() {
        LazyJsonValue age4 = new LazyJsonValue("{ \"age\": 4 }");
        JsonObject nonLazyReference = age4.asObject();
        nonLazyReference.set("age", 5);
        assertEquals(5, age4.asObject().get("age").asInt());
    }
}
