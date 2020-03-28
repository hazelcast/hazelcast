/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import testsubjects.NonStaticFunctionFactory;
import testsubjects.StaticNonSerializableBiFunction;
import testsubjects.StaticNonSerializableFunction;
import testsubjects.StaticSerializableBiFunction;
import testsubjects.StaticSerializableFunction;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ComputeConditionallyTests extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    HazelcastInstance[] instances;
    private HazelcastInstance firstNode;
    private HazelcastInstance secondNode;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = getConfig();

        instances = factory.newInstances(config);
        firstNode = instances[0];
        secondNode = instances[1];
    }

    @Test
    public void testComputeIfPresentWithLambdaReMappingFunction() {
        final String outer_state = "outer_state";
        BiFunction biFunction = (key, oldValue) -> "new_value_from_lambda_and_" + outer_state;
        testComputeIfPresentForFunction(biFunction, "new_value_from_lambda_and_outer_state");
    }


    @Test
    public void testComputeIfPresentWithAnonymousReMappingFunction() {
        BiFunction<String, String, String> biFunction = NonStaticFunctionFactory
                .getAnonymousNonSerializableBiFunction("new_value");
        testComputeIfPresentForFunction(biFunction, "new_value");
    }

    @Test
    public void testComputeIfPresentWithStaticSerializableRemappingFunction() {
        StaticSerializableBiFunction biFunction = new StaticSerializableBiFunction("new_value");
        testComputeIfPresentForFunction(biFunction, "new_value");
    }

    @Test
    public void testComputeIfPresentWithStaticNonSerializableRemappingFunction() {
        StaticNonSerializableBiFunction biFunction = new StaticNonSerializableBiFunction("new_value");
        testComputeIfPresentForFunction(biFunction, "new_value");
    }

    @Test
    public void testComputeIfPresentWithLambdaReMappingFunction_AndInPlaceModification() {
        final IMap<String, AtomicBoolean> map = firstNode.getMap("testComputeIfPresent");
        map.put("present_key", new AtomicBoolean(true));
        AtomicBoolean newValue = map.computeIfPresent("present_key", (k, o) -> {
                    o.set(false);
                    return o;
                }
        );
        assertFalse(newValue.get());
        assertFalse(map.get("present_key").get());
    }

    @Test
    public void testComputeIfPresentShouldRemoveValueWhenRemappingFunctionReturnsNull() {
        BiFunction biFunction = (key, oldValue) -> null;
        testComputeIfPresentForFunction(biFunction, null);
    }

    @Test
    public void testComputeIfPresentShouldNotExecuteRemappingFunctionForAbsentKeys() {
        final IMap<String, String> map = firstNode.getMap("testComputeIfPresent");
        map.computeIfPresent("absent_key", (key, oldValue) -> {
            fail("should not be called");
            return "test_value";
        });
    }

    private void testComputeIfPresentForFunction(BiFunction testFunction, Object expectedValue) {
        testComputeIfPresentForFunction(firstNode, testFunction, expectedValue);
        testComputeIfPresentForFunction(secondNode, testFunction, expectedValue);
    }

    private void testComputeIfPresentForFunction(HazelcastInstance hz, BiFunction testFunction, Object expectedValue) {
        final IMap<String, String> map = hz.getMap("testComputeIfPresent" + hz.getName());
        map.put("present_key", "old_value");

        String newValue = map.computeIfPresent("present_key", testFunction);

        assertEquals(expectedValue, newValue);
        assertEquals(expectedValue, map.get("present_key"));
        if (expectedValue == null) {
            assertFalse(map.containsKey("present_key"));
        }
    }

    @Test
    public void testComputeIfAbsentWithLambdaMappingFunction() {
        final String outer_state = "outer_state";
        Function function = (key) -> "new_value_from_lambda_and_" + outer_state;
        testComputeIfAbsentForFunction(function, "new_value_from_lambda_and_outer_state");
    }


    @Test
    public void testComputeIfAbsentWithAnonymousMappingFunction() {
        Function<String, String> function = NonStaticFunctionFactory
                .getAnonymousNonSerializableFunction("new_value");
        testComputeIfAbsentForFunction(function, "new_value");
    }

    @Test
    public void testComputeIfAbsentWithStaticSerializableMappingFunction() {
        StaticSerializableFunction function = new StaticSerializableFunction("new_value");
        testComputeIfAbsentForFunction(function, "new_value");
    }

    @Test
    public void testComputeIfAbsentWithStaticNonSerializableMappingFunction() {
        StaticNonSerializableFunction function = new StaticNonSerializableFunction("new_value");
        testComputeIfAbsentForFunction(function, "new_value");
    }

    @Test
    public void testComputeIfAbsentShouldReturnNullWhenMappingFunctionReturnsNull() {
        Function function = (k) -> null;
        testComputeIfAbsentForFunction(function, null);
    }

    @Test
    public void testComputeIfAbsentShouldReturnExistingValueWhenItExists() {
        final IMap<String, String> map = firstNode.getMap("testComputeIfAbsent");
        map.put("present_key", "present_value");

        Function function = (k) -> "new_value";
        assertEquals(map.computeIfAbsent("present_key", function), "present_value");
        assertEquals(map.get("present_key"), "present_value");
    }

    @Test
    public void testComputeIfAbsentShouldNotExecuteMappingFunctionForPresentKeys() {
        final IMap<String, String> map = firstNode.getMap("testComputeIfAbsent");
        map.put("present_key", "present_value");
        map.computeIfAbsent("present_key", (key) -> {
            fail("should not be called");
            return "test_value";
        });
    }

    private void testComputeIfAbsentForFunction(Function testFunction, Object expectedValue) {
        testComputeIfAbsentForFunction(firstNode, testFunction, expectedValue);
        testComputeIfAbsentForFunction(secondNode, testFunction, expectedValue);
    }

    private void testComputeIfAbsentForFunction(HazelcastInstance hz, Function testFunction, Object expectedValue) {
        final IMap<String, String> map = hz.getMap("testComputeIfAbsent" + hz.getName());
        String newValue = map.computeIfAbsent("absent_key", testFunction);

        assertEquals(expectedValue, map.get("absent_key"));
        assertEquals(expectedValue, newValue);
    }

}
