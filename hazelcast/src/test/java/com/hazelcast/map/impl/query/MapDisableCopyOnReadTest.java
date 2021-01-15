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

package com.hazelcast.map.impl.query;

import com.google.common.collect.Sets;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapDisableCopyOnReadTest extends HazelcastTestSupport {

    @Parameters(name = "isImmutable:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {ImmutabilityType.CONFIG},
                {ImmutabilityType.ANNOTATION},
                {ImmutabilityType.NONE},
        });
    }

    @Parameter
    public ImmutabilityType immutableType;

    private PartitionService partitionService;
    private MapProxyImpl<Object, Object> map;

    @Before
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setup() {
        MapConfig mapConfig = new MapConfig("mappy").setInMemoryFormat(InMemoryFormat.OBJECT);
        if (immutableType == ImmutabilityType.CONFIG) {
            mapConfig.setImmutableValues(true);
        }

        Config config = smallInstanceConfig().addMapConfig(mapConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        this.partitionService = instance.getPartitionService();
        this.map = (MapProxyImpl) instance.getMap("mappy");
    }

    @Test
    public void testGet() {
        testImmutable(immutableType, map::get);
    }

    @Test
    public void testGetAll() {
        testImmutable(immutableType, key -> map.getAll(Sets.newHashSet(key)).values().iterator().next());
    }

    @Test
    public void testEP() {
        testImmutable(immutableType, key -> map.executeOnKey(key, Entry::getValue));
    }

    @Test
    public void testMapIterator() {
        testImmutable(immutableType, key -> {
            int id = partitionService.getPartition(key).getPartitionId();
            return map.iterator(10, id, false).next().getValue();
        });
    }

    private void testImmutable(ImmutabilityType immutableType, Function<Object, Object> entrySupplier) {
        Object obj = immutableType == ImmutabilityType.ANNOTATION ? new TestEntityImmutable("hello") : new TestEntity("hello");
        map.put(obj, obj);
        if (immutableType == ImmutabilityType.NONE) {
            assertNotSame(entrySupplier.apply(obj), entrySupplier.apply(obj));
        } else {
            assertSame(entrySupplier.apply(obj), entrySupplier.apply(obj));
        }
    }

    public enum ImmutabilityType {
        CONFIG,
        ANNOTATION,
        NONE
    }
}
