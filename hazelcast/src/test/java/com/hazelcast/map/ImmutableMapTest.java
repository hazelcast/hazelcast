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
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ImmutableMapTest extends HazelcastTestSupport {

    private transient HazelcastInstance local;
    private transient HazelcastInstance remote;
    private static final String OBJECT_MAP = "OBJECT_MAP";
    private static final String BINARY_MAP = "BINARY_MAP";
    private static Config config;

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
    }

    @After
    public void destroy() {
        local.shutdown();
        remote.shutdown();
    }

    @Test
    public void testSerialisationOnLocalPutAndGet() {
        checkPutAndGet(local, Assert::assertTrue);
    }

    @Test
    public void testSerialisationOnRemotePutAndGet() {
        checkPutAndGet(remote, Assert::assertFalse);
    }

    @Test
    public void testSerialisationOnLocalPutAndRemove() {
        checkPutAndRemove(local, Assert::assertTrue);
    }

    @Test
    public void testSerialisationOnRemotePutAndRemove() {
        checkPutAndRemove(remote, Assert::assertFalse);
    }

    private void checkPutAndGet(HazelcastInstance hz, Consumer<Boolean> checkAssertion) {

        String putKey = generateKeyOwnedBy(hz);
        String putIfAbsentKey = generateKeyOwnedBy(hz);

        ConstantString constantValue = new ConstantString("I am a constant");
        ConstantString differentConstantValue = new ConstantString("I am a different constant");

        BiFunction<IMap<String, ConstantString>, ConstantString, ConstantString> constantPutFx =
            (map, value) -> map.put(putKey, value);
        BiFunction<IMap<String, ConstantString>, ConstantString, ConstantString> constantPutIfAbsentFx =
            (map, value) -> map.putIfAbsent(putIfAbsentKey, value);

        IMap<String, ConstantString> constantBinaryMap = local.getMap(randomMapName(BINARY_MAP));
        checkPutGetForMap(putIfAbsentKey, constantValue, differentConstantValue, constantBinaryMap, constantPutIfAbsentFx,
            Assert::assertFalse);
        checkPutGetForMap(putKey, constantValue, differentConstantValue, constantBinaryMap, constantPutFx, Assert::assertFalse);

        IMap<String, ConstantString> constantObjectMap = local.getMap(randomMapName(OBJECT_MAP));
        checkPutGetForMap(putIfAbsentKey, constantValue, differentConstantValue, constantObjectMap, constantPutIfAbsentFx,
            checkAssertion);
        checkPutGetForMap(putKey, constantValue, differentConstantValue, constantObjectMap, constantPutFx, checkAssertion);

        VariableString variableValue = new VariableString("I am a variable string");
        VariableString differentVariableValue = new VariableString("I am a different variable string");

        BiFunction<IMap<String, VariableString>, VariableString, VariableString> variablePutFx =
            (map, value) -> map.put(putKey, value);
        BiFunction<IMap<String, VariableString>, VariableString, VariableString> variablePutIfAbsentFx =
            (map, value) -> map.putIfAbsent(putIfAbsentKey, value);

        IMap<String, VariableString> variableBinaryMap = local.getMap(randomMapName(BINARY_MAP));
        checkPutGetForMap(putIfAbsentKey, variableValue, differentVariableValue, variableBinaryMap, variablePutIfAbsentFx,
            Assert::assertFalse);
        checkPutGetForMap(putKey, variableValue, differentVariableValue, variableBinaryMap, variablePutFx, Assert::assertFalse);

        IMap<String, VariableString> variableObjectMap = local.getMap(randomMapName(OBJECT_MAP));
        checkPutGetForMap(putIfAbsentKey, variableValue, differentVariableValue, variableObjectMap, variablePutIfAbsentFx,
            Assert::assertFalse);
        checkPutGetForMap(putKey, variableValue, differentVariableValue, variableObjectMap, variablePutFx, Assert::assertFalse);
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

    private void checkPutAndRemove(HazelcastInstance hz, Consumer<Boolean> checkAssertion) {

        String key = generateKeyOwnedBy(hz);
        ConstantString constantValue = new ConstantString("I am a constant");

        IMap<String, ConstantString> constantBinaryMap = local.getMap(randomMapName(BINARY_MAP));
        checkPutRemoveForMap(key, constantValue, constantBinaryMap, Assert::assertFalse);

        IMap<String, ConstantString> constantObjectMap = local.getMap(randomMapName(OBJECT_MAP));
        checkPutRemoveForMap(key, constantValue, constantObjectMap, checkAssertion);

        VariableString variableValue = new VariableString("I am a variable string");

        IMap<String, VariableString> variableBinaryMap = local.getMap(randomMapName(BINARY_MAP));
        checkPutRemoveForMap(key, variableValue, variableBinaryMap, Assert::assertFalse);

        IMap<String, VariableString> variableObjectMap = local.getMap(randomMapName(OBJECT_MAP));
        checkPutRemoveForMap(key, variableValue, variableObjectMap, Assert::assertFalse);
    }

    private void checkPutRemoveForMap(String key, Object value, IMap map, Consumer<Boolean> checkAssertion) {

        map.put(key, value);
        Object removedValue = map.remove(key);
        assertEquals(value, removedValue);
        checkAssertion.accept(value == removedValue);
    }


    // containsValue
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
