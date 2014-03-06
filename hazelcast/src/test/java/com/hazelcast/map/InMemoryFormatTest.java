/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static junit.framework.Assert.assertNotSame;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InMemoryFormatTest extends HazelcastTestSupport {

    @Test
    public void equals() {
        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(InMemoryFormat.OBJECT));
        config.addMapConfig(new MapConfig("binaryMap").setInMemoryFormat(InMemoryFormat.BINARY));

        HazelcastInstance hz = createHazelcastInstance(config);

        Pair v1 = new Pair("a", "1");
        Pair v2 = new Pair("a", "2");

        IMap<String, Pair> objectMap = hz.getMap("objectMap");
        IMap<String, Pair> binaryMap = hz.getMap("binaryMap");

        objectMap.put("1", v1);
        binaryMap.put("1", v1);

        assertTrue(objectMap.containsValue(v1));
        assertTrue(objectMap.containsValue(v2));

        assertTrue(binaryMap.containsValue(v1));
        assertFalse(binaryMap.containsValue(v2));
    }

    @Test
    public void equalsReadLocalBackup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(InMemoryFormat.OBJECT).setReadBackupData(true));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        Pair pair = new Pair("a", "1");

        IMap<String, Pair> objectMap1 = hz1.getMap("objectMap");
        IMap<String, Pair> objectMap2 = hz2.getMap("objectMap");

        objectMap1.put("1", pair);
        Pair v1 = objectMap1.get("1");
        Pair v2 = objectMap1.get("1");

        Pair rv1 = objectMap2.get("1");
        Pair rv2 = objectMap2.get("1");
        assertNotSame(pair, v1);
        assertNotSame(pair, v2);
        assertNotSame(v1, v2);

        assertNotSame(pair, rv1);
        assertNotSame(pair, rv2);
        assertNotSame(rv1, rv2);

        assertTrue(objectMap2.containsValue(v1));
    }

    public static final class Pair implements Serializable {
        private final String significant;
        private final String insignificant;

        public Pair(String significant, String insignificant) {
            this.significant = significant;
            this.insignificant = insignificant;
        }

        @Override
        public boolean equals(Object thatObj) {
            if (this == thatObj) {
                return true;
            }
            if (thatObj == null || getClass() != thatObj.getClass()) {
                return false;
            }
            Pair that = (Pair) thatObj;
            return this.significant.equals(that.significant);
        }

        @Override
        public int hashCode() {
            return significant.hashCode();
        }
    }

}
