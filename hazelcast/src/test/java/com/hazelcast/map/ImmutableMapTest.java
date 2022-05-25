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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ImmutableMapTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private HazelcastInstance remote;
    private static final String OBJECT_MAP = "OBJECT_MAP";
    private static final String BINARY_MAP = "BINARY_MAP";
    private static final ConstantString CONSTANT_VALUE = new ConstantString("I am a constant");
    private static final VariableString VARIABLE_VALUE = new VariableString("I am a variable string");
    private static Config config;

    private String localKey;
    private String remoteKey;
    private IMap<String, ConstantString> constantBinaryMap;
    private IMap<String, ConstantString> constantObjectMap;
    private IMap<String, VariableString> variableBinaryMap;
    private IMap<String, VariableString> variableObjectMap;


    @BeforeClass
    public static void initConfig() {

        config = new Config();
        config.addMapConfig(new MapConfig(OBJECT_MAP + "*").setInMemoryFormat(InMemoryFormat.OBJECT));
        config.addMapConfig(new MapConfig(BINARY_MAP + "*").setInMemoryFormat(InMemoryFormat.BINARY));
    }

    @Before
    public void setUp() {

        HazelcastInstance[] instances = createHazelcastInstances(config, 2);
        local = instances[0];
        remote = instances[1];

        localKey = generateKeyOwnedBy(local);
        remoteKey = generateKeyOwnedBy(remote);
        constantBinaryMap = local.getMap(randomMapName(BINARY_MAP));
        constantObjectMap = local.getMap(randomMapName(OBJECT_MAP));
        variableBinaryMap = local.getMap(randomMapName(BINARY_MAP));
        variableObjectMap = local.getMap(randomMapName(OBJECT_MAP));
    }

    @After
    public void destroy() {
        local.shutdown();
        remote.shutdown();
    }

    @Test
    public void testSerialisationOnPutGet() {
        checkPutAndGet(this::checkPutGet);
    }

    @Test
    public void testSerialisationOnPutIfAbsentGet() {
        checkPutAndGet(this::checkPutIfAbsentGet);
    }

    @Test
    public void testSerialisationOnPutGetAsync() {
        checkPutAndFoo(this::checkPutGetAsync);
    }

    @Test
    public void testSerialisationOnPutRemove() {
        checkPutAndFoo(this::checkPutRemove);
    }

    @Test
    public void testSerialisationOnPutRemoveKV() {
        checkPutAndFoo(this::checkPutRemoveKV);
    }

    @Test
    public void testSerialisationOnPutContainsValue() {
        checkPutAndFoo(this::checkPutContainsValue);
    }

    private <T> void checkPutAndGet(Consumer5<T> func) {

        Consumer5<ConstantString> funcConstString = (Consumer5<ConstantString>) func;
        Consumer5<VariableString> funcVarString = (Consumer5<VariableString>) func;

        ConstantString differentConstantValue = new ConstantString("I am a different constant");
        VariableString differentVariableValue = new VariableString("I am a different variable string");

        try {

            // Immutable value on a local map with InMemoryFormat.OBJECT should not serialize/deserialize.
            // Serialisation should happen in all other cases
            funcConstString.accept(localKey, CONSTANT_VALUE, differentConstantValue, constantBinaryMap, false);
            funcConstString.accept(localKey, CONSTANT_VALUE, differentConstantValue, constantObjectMap, true);
            funcVarString.accept(localKey, VARIABLE_VALUE, differentVariableValue, variableBinaryMap, false);
            funcVarString.accept(localKey, VARIABLE_VALUE, differentVariableValue, variableObjectMap, false);

            funcConstString.accept(remoteKey, CONSTANT_VALUE, differentConstantValue, constantBinaryMap, false);
            funcConstString.accept(remoteKey, CONSTANT_VALUE, differentConstantValue, constantObjectMap, false);
            funcVarString.accept(remoteKey, VARIABLE_VALUE, differentVariableValue, variableBinaryMap, false);
            funcVarString.accept(remoteKey, VARIABLE_VALUE, differentVariableValue, variableObjectMap, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> void checkPutGet(String key, T value1, T value2, IMap<String, T> map, boolean checkAssertion) {

        map.put(key, value1);
        T getValue = map.get(key);
        assertEquals(value1, getValue);
        if (checkAssertion) {
            assertTrue(value1 == getValue);
        } else {
            assertFalse(value1 == getValue);
        }

        T oldValue = map.put(key, value2);
        assertEquals(value1, oldValue);
        if (checkAssertion) {
            assertTrue(value1 == oldValue);
        } else {
            assertFalse(value1 == oldValue);
        }
    }

    private <T> void checkPutIfAbsentGet(String key, T value1, T value2, IMap<String, T> map, boolean checkAssertion) {

        map.putIfAbsent(key, value1);
        T getValue = map.get(key);
        assertEquals(value1, getValue);
        if (checkAssertion) {
            assertTrue(value1 == getValue);
        } else {
            assertFalse(value1 == getValue);
        }

        T oldValue = map.putIfAbsent(key, value2);
        assertEquals(value1, oldValue);
        if (checkAssertion) {
            assertTrue(value1 == oldValue);
        } else {
            assertFalse(value1 == oldValue);
        }

        T nonReplacedValue = map.get(key);
        assertEquals(value1, nonReplacedValue);
        if (checkAssertion) {
            assertTrue(value1 == nonReplacedValue);
        } else {
            assertFalse(value1 == nonReplacedValue);
        }
    }

    private <T> void checkPutAndFoo(Consumer4<T> func) {

        Consumer4<ConstantString> funcConstString = (Consumer4<ConstantString>) func;
        Consumer4<VariableString> funcVarString = (Consumer4<VariableString>) func;

        try {

            // Immutable value on a local map with InMemoryFormat.OBJECT should not serialize/deserialize.
            // Serialisation should happen in all other cases
            funcConstString.accept(localKey, CONSTANT_VALUE, constantBinaryMap, false);
            funcConstString.accept(localKey, CONSTANT_VALUE, constantObjectMap, true);
            funcVarString.accept(localKey, VARIABLE_VALUE, variableBinaryMap, false);
            funcVarString.accept(localKey, VARIABLE_VALUE, variableObjectMap, false);

            funcConstString.accept(remoteKey, CONSTANT_VALUE, constantBinaryMap, false);
            funcConstString.accept(remoteKey, CONSTANT_VALUE, constantObjectMap, false);
            funcVarString.accept(remoteKey, VARIABLE_VALUE, variableBinaryMap, false);
            funcVarString.accept(remoteKey, VARIABLE_VALUE, variableObjectMap, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> void checkPutRemove(String key, T value, IMap<String, T> map, boolean checkAssertion) {

        map.put(key, value);
        T removedValue = map.remove(key);
        assertEquals(value, removedValue);
        if (checkAssertion) {
            assertTrue(value == removedValue);
        } else {
            assertFalse(value == removedValue);
        }
    }

    private <T> void checkPutRemoveKV(String key, T value, IMap<String, T> map, boolean throwAway) {

        map.put(key, value);
        assertTrue(map.remove(key, value));
        assertNull(map.get(key));
    }

    private <T> void checkPutContainsValue(String key, T value, IMap<String, T> map, boolean checkAssertion) {

        map.put(key, value);
        assertTrue(map.containsValue(value));
        T getValue = map.get(key);
        assertEquals(value, getValue);
        if (checkAssertion) {
            assertTrue(value == getValue);
        } else {
            assertFalse(value == getValue);
        }
    }

    private <T> void checkPutGetAsync(String key, T value, IMap<String, T> map, boolean checkAssertion)
        throws ExecutionException, InterruptedException {

        map.put(key, value);
        T getValue = map.getAsync(key).toCompletableFuture().get();
        assertEquals(value, getValue);
        if (checkAssertion) {
            assertTrue(value == getValue);
        } else {
            assertFalse(value == getValue);
        }
    }

    public static class ConstantString implements DataSerializable, Immutable {
        private String str;
        ConstantString(String str) {
            this.str = str;
        }

        ConstantString() { }

        private void setString(String str) {
            this.str = str;
        }

        public String getString() {
            return str;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConstantString that = (ConstantString) o;
            return Objects.equals(str, that.getString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(str);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(str);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            str = in.readString();
        }
    }

    public static class VariableString implements DataSerializable {
        private String str;

        VariableString(String str) {
            this.str = str;
        }

        VariableString() { }

        public void setString(String str) {
            this.str = str;
        }

        public String getString() {
            return str;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VariableString that = (VariableString) o;
            return Objects.equals(str, that.getString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(str);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(str);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            str = in.readString();
        }
    }

    @FunctionalInterface
    interface Consumer4<T> {
        void accept(String key, T value, IMap<String, T> map, boolean condition) throws Exception;
    }

    @FunctionalInterface
    interface Consumer5<T> {
        void accept(String key, T value1, T value2, IMap<String, T> map, boolean condition) throws Exception;
    }
}
