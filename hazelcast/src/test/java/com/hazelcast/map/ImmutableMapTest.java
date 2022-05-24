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
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.junit.Assert;
import static org.junit.Assert.assertTrue;
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
    public void testSerialisationOnLocalPutAndGet() {
        checkPutAndGet(localKey, generateKeyOwnedBy(local), Assert::assertTrue);
    }

    @Test
    public void testSerialisationOnRemotePutAndGet() {
        checkPutAndGet(remoteKey, generateKeyOwnedBy(remote), Assert::assertFalse);
    }

    @Test
    public void testSerialisationOnLocalPutAndRemove() {
        checkPutAndRemove(localKey, Assert::assertTrue);
    }

    @Test
    public void testSerialisationOnRemotePutAndRemove() {
        checkPutAndRemove(remoteKey, Assert::assertFalse);
    }

    @Test
    public void testSerialisationOnLocalPutAndContainsValue() {
        checkPutAndContainsValue(localKey, Assert::assertTrue);
    }

    @Test
    public void testSerialisationOnRemotePutAndContainsValue() {
        checkPutAndContainsValue(remoteKey, Assert::assertFalse);
    }

    private void checkPutAndGet(String putKey, String putIfAbsentKey, Consumer<Boolean> checkAssertion) {

        ConstantString differentConstantValue = new ConstantString("I am a different constant");

        BiFunction<IMap<String, ConstantString>, ConstantString, ConstantString> constantPutFx =
            (map, value) -> map.put(putKey, value);
        BiFunction<IMap<String, ConstantString>, ConstantString, ConstantString> constantPutIfAbsentFx =
            (map, value) -> map.putIfAbsent(putIfAbsentKey, value);

        checkPutGetForMap(putIfAbsentKey, CONSTANT_VALUE, differentConstantValue, constantBinaryMap, constantPutIfAbsentFx,
            Assert::assertFalse);
        checkPutGetForMap(putKey, CONSTANT_VALUE, differentConstantValue, constantBinaryMap, constantPutFx, Assert::assertFalse);

        checkPutGetForMap(putIfAbsentKey, CONSTANT_VALUE, differentConstantValue, constantObjectMap, constantPutIfAbsentFx,
            checkAssertion);
        checkPutGetForMap(putKey, CONSTANT_VALUE, differentConstantValue, constantObjectMap, constantPutFx, checkAssertion);

        VariableString differentVariableValue = new VariableString("I am a different variable string");

        BiFunction<IMap<String, VariableString>, VariableString, VariableString> variablePutFx =
            (map, value) -> map.put(putKey, value);
        BiFunction<IMap<String, VariableString>, VariableString, VariableString> variablePutIfAbsentFx =
            (map, value) -> map.putIfAbsent(putIfAbsentKey, value);

        checkPutGetForMap(putIfAbsentKey, VARIABLE_VALUE, differentVariableValue, variableBinaryMap, variablePutIfAbsentFx,
            Assert::assertFalse);
        checkPutGetForMap(putKey, VARIABLE_VALUE, differentVariableValue, variableBinaryMap, variablePutFx, Assert::assertFalse);

        checkPutGetForMap(putIfAbsentKey, VARIABLE_VALUE, differentVariableValue, variableObjectMap, variablePutIfAbsentFx,
            Assert::assertFalse);
        checkPutGetForMap(putKey, VARIABLE_VALUE, differentVariableValue, variableObjectMap, variablePutFx, Assert::assertFalse);
    }

    private <T> void checkPutGetForMap(String key, T value1, T value2, IMap<String, T> map,
                                       BiFunction<IMap<String, T>, T, T> putMethod, Consumer<Boolean> checkAssertion) {

        putMethod.apply(map, value1);
        T getValue = map.get(key);
        assertEquals(value1, getValue);
        checkAssertion.accept(value1 == getValue);

        T oldValue = putMethod.apply(map, value2);
        assertEquals(value1, oldValue);
        checkAssertion.accept(value1 == oldValue);
    }

    private void checkPutAndRemove(String key, Consumer<Boolean> checkAssertion) {

        checkPutRemoveForMap(key, CONSTANT_VALUE, constantBinaryMap, Assert::assertFalse);

        checkPutRemoveForMap(key, CONSTANT_VALUE, constantObjectMap, checkAssertion);

        checkPutRemoveForMap(key, VARIABLE_VALUE, variableBinaryMap, Assert::assertFalse);

        checkPutRemoveForMap(key, VARIABLE_VALUE, variableObjectMap, Assert::assertFalse);
    }

    private <T> void checkPutRemoveForMap(String key, T value, IMap<String, T> map, Consumer<Boolean> checkAssertion) {

        map.put(key, value);
        T removedValue = map.remove(key);
        assertEquals(value, removedValue);
        checkAssertion.accept(value == removedValue);
    }

    private void checkPutAndContainsValue(String key, Consumer<Boolean> checkAssertion) {

        checkPutContainsValueForMap(key, CONSTANT_VALUE, constantBinaryMap, Assert::assertFalse);

        checkPutContainsValueForMap(key, CONSTANT_VALUE, constantObjectMap, checkAssertion);

        checkPutContainsValueForMap(key, VARIABLE_VALUE, variableBinaryMap, Assert::assertFalse);

        checkPutContainsValueForMap(key, VARIABLE_VALUE, variableObjectMap, Assert::assertFalse);
    }

    private <T> void checkPutContainsValueForMap(String key, T value, IMap<String, T> map, Consumer<Boolean> checkAssertion) {

        map.put(key, value);
        assertTrue(map.containsValue(value));
        T getValue = map.get(key);
        assertEquals(value, getValue);
        checkAssertion.accept(value == getValue);
    }

    // containsValue - Done
    // remove(k, v)
    // delete
    // getAsync
    // putAsync
    // putAsync(ttl)
    // putAsync(ttl, idleTime)
    // setAsync  - Does not return a value
    // setAsync(ttl) - Does not return a value
    // setAsync(ttl, idleTime) - Does not return a value
    // removeAsync
    // tryRemove
    // tryPut(timeout) - Does not return value
    // put(ttl) - calls putInternal
    // put(ttl, idleTime) - calls putInternal
    // putTransient - Does not return a value
    // putTransient(idleTime) - Does not return a value
    // putIfAbsent()
    // putIfAbsent(ttl)
    // putIfAbsent(ttl, idleTime)
    // replace(old, new)
    // replace()
    // set - Does not return a value
    // replace

//    @Test
//    public void testConstantStringSeraliseOnPut() {
//
//        String key = "1";
//
//        ConstantString example = new ConstantString("example");
//        objectMap.put(key, example);
//        binaryMap.put(key, example);
//
//        objectMap.put()
//
//        ConstantString objectGet = objectMap.get(key);
//        ConstantString binaryGet = binaryMap.get(key);
//
//        assertEquals(example, objectGet);
//        assertEquals(example, binaryGet);
//
//        assertFalse(example == binaryGet);
//    //    assertTrue(example == objectGet);
//
//        binaryGet = binaryMap.put(key, new ConstantString("Hello"));
//        objectGet = objectMap.put(key, new ConstantString("Hello"));
//
//        assertEquals(example, objectGet);
//        assertEquals(example, binaryGet);
//
//        assertFalse(example == binaryGet);
//    //    assertTrue(example == objectGet);
//
//        // 3 puts, entrySet, get, multinode, putAll, getAll, executeOnKey, executeOnKeys, submitOnKey, submitOnKeys, remove, aggregate
//
//        // IF backup reads are allowed - check multinode
//        // Near cache reads - check for defensive copy
//
//    }

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
}
