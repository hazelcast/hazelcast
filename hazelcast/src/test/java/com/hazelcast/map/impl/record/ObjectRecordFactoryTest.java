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

package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectRecordFactoryTest extends AbstractRecordFactoryTest<Object> {

    @Parameterized.Parameters(name = "perEntryStatsEnabled:{0}, evictionPolicy:{1}, cacheDeserializedValues:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, EvictionPolicy.NONE, CacheDeserializedValues.NEVER, ObjectRecordWithStats.class},
                {true, EvictionPolicy.LFU, CacheDeserializedValues.ALWAYS, ObjectRecordWithStats.class},
                {false, EvictionPolicy.NONE, CacheDeserializedValues.NEVER, SimpleRecord.class},
                {false, EvictionPolicy.NONE, CacheDeserializedValues.ALWAYS, SimpleRecord.class},
                {false, EvictionPolicy.LFU, CacheDeserializedValues.NEVER, SimpleRecordWithLFUEviction.class},
                {false, EvictionPolicy.LFU, CacheDeserializedValues.ALWAYS, SimpleRecordWithLFUEviction.class},
                {false, EvictionPolicy.LRU, CacheDeserializedValues.NEVER, SimpleRecordWithLRUEviction.class},
                {false, EvictionPolicy.LRU, CacheDeserializedValues.ALWAYS, SimpleRecordWithLRUEviction.class},
                {false, EvictionPolicy.RANDOM, CacheDeserializedValues.NEVER, SimpleRecord.class},
                {false, EvictionPolicy.RANDOM, CacheDeserializedValues.ALWAYS, SimpleRecord.class},
        });
    }

    @Override
    protected RecordFactory newRecordFactory() {
        MapContainer mapContainer = createMapContainer(perEntryStatsEnabled,
                evictionPolicy, cacheDeserializedValues);
        return new ObjectRecordFactory(mapContainer, serializationService);
    }
}
