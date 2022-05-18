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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Objects;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ImmutableMapTest extends HazelcastTestSupport {

    @Test
    public void testConstantStringDoesNotSeraliseOnPut() {

        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(InMemoryFormat.OBJECT));
        config.addMapConfig(new MapConfig("binaryMap").setInMemoryFormat(InMemoryFormat.BINARY));

        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<String, ConstantString> objectMap = hz.getMap("objectMap");
        IMap<String, ConstantString> binaryMap = hz.getMap("binaryMap");

        String key = "1";

        ConstantString example = new ConstantString("example");
        objectMap.put(key, example);
        binaryMap.put(key, example);

        ConstantString objectGet = objectMap.get(key);
        ConstantString binaryGet = binaryMap.get(key);

        assertEquals(example, objectGet);
        assertEquals(example, binaryGet);

        assertFalse(example == binaryGet);
        assertTrue(example == objectGet);

        binaryGet = binaryMap.put(key, new ConstantString("Hello"));
        objectGet = objectMap.put(key, new ConstantString("Hello"));

        assertEquals(example, objectGet);
        assertEquals(example, binaryGet);

        assertFalse(example == binaryGet);
        assertTrue(example == objectGet);

        // 3 puts, entrySet, get, multinode, putAll, getAll, executeOnKey, executeOnKeys, submitOnKey, submitOnKeys, remove, aggregate

        // IF backup reads are allowed - check multinode
        // Near cache reads - check for defensive copy

    }

    @Test
    public void testConstantStringDoesNotSeraliseOnPutAll() {

        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(InMemoryFormat.OBJECT));
        config.addMapConfig(new MapConfig("binaryMap").setInMemoryFormat(InMemoryFormat.BINARY));

        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<String, ConstantString> objectMap = hz.getMap("objectMap");
        IMap<String, ConstantString> binaryMap = hz.getMap("binaryMap");

        String key = "1";

        ConstantString example = new ConstantString("example");
        objectMap.put(key, example);
        binaryMap.put(key, example);

        ConstantString objectGet = objectMap.get(key);
        ConstantString binaryGet = binaryMap.get(key);

        assertEquals(example, objectGet);
        assertEquals(example, binaryGet);

        assertFalse(example == binaryGet);
        assertTrue(example == objectGet);

        binaryGet = binaryMap.put(key, new ConstantString("Hello"));
        objectGet = objectMap.put(key, new ConstantString("Hello"));

        assertEquals(example, objectGet);
        assertEquals(example, binaryGet);

        assertFalse(example == binaryGet);
        assertTrue(example == objectGet);

        // 3 puts, entrySet, get, multinode, putAll, getAll, executeOnKey, executeOnKeys, submitOnKey, submitOnKeys, remove, aggregate

        // IF backup reads are allowed - check multinode
        // Near cache reads - check for defensive copy

    }
}

class ConstantString implements Immutable, DataSerializable {

    private String str;
    ConstantString(String str) {
        this.str = str;
    }

    ConstantString() { }

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
        ConstantString that = (ConstantString) o;
        return Objects.equals(str, that.str);
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
        setString(in.readString());
    }
}
