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

package com.hazelcast.internal.serialization.impl.compact.extractor;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.finger;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.limb;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.person;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.tattoos;
import static com.hazelcast.query.Predicates.sql;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryTest extends HazelcastTestSupport {

    private static final ComplexTestDataStructure.Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    @Test
    public void tesObjectInMemoryFormatSupported_withoutClassConfig() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.addMapConfig(mapConfig);
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap map = instance.getMap("map");
        map.put(1, BOND);

        map.values(sql("name == Bond"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexNotSupportedForObjectAttributes() throws IOException {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));

        IndexConfig indexConfig = new IndexConfig();
        indexConfig.addAttribute("firstLimb");
        indexConfig.setType(IndexType.SORTED);
        mapConfig.addIndexConfig(indexConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap map = instance.getMap("map");
        map.put(1, BOND);
    }

    @Test
    public void testHashIndexForComparableAttributes() throws IOException {
        testIndexForComparableAttributes(IndexType.HASH);
    }

    @Test
    public void testBitMapIndexForComparableAttributes() throws IOException {
        testIndexForComparableAttributes(IndexType.BITMAP);
    }

    @Test
    public void testSortedIndexForComparableAttributes() throws IOException {
        testIndexForComparableAttributes(IndexType.SORTED);
    }

    public void testIndexForComparableAttributes(IndexType indexType) throws IOException {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));

        IndexConfig indexConfig = new IndexConfig();
        indexConfig.addAttribute("firstLimb.name");
        indexConfig.setType(indexType);
        mapConfig.addIndexConfig(indexConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap map = instance.getMap("map");
        for (int i = 0; i < 10; i++) {
            map.put(i, BOND);
        }

        Collection result = map.values(Predicates.equal("firstLimb.name", "left-hand"));
        assertEquals(10, result.size());
        for (Object value : result) {
            ComplexTestDataStructure.Limb firstLimb = ((ComplexTestDataStructure.Person) value).firstLimb;
            assertEquals("left-hand", firstLimb.name);
        }
    }
}
