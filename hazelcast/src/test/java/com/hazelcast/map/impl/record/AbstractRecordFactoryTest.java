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
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractRecordFactoryTest<T> extends HazelcastTestSupport {

    @Parameterized.Parameter(0)
    public boolean perEntryStatsEnabled;
    @Parameterized.Parameter(1)
    public EvictionPolicy evictionPolicy;
    @Parameterized.Parameter(2)
    public CacheDeserializedValues cacheDeserializedValues;
    @Parameterized.Parameter(3)
    public Class expectedRecordClass;

    RecordFactory factory;
    SerializationService serializationService;

    @Before
    public void setUp() throws Exception {
        serializationService = createSerializationService();
        factory = newRecordFactory();
    }

    protected abstract RecordFactory newRecordFactory();

    @Test
    public void test_expected_record_per_config_is_created() {
        Data key = serializationService.toData("key");
        Record record = factory.newRecord(key, "value");
        assertEquals(expectedRecordClass.getCanonicalName(), record.getClass().getCanonicalName());
    }

    InternalSerializationService createSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    protected MapContainer createMapContainer(boolean perEntryStatsEnabled,
                                              EvictionPolicy evictionPolicy,
                                              CacheDeserializedValues cacheDeserializedValues) {
        MapConfig mapConfig = newMapConfig(perEntryStatsEnabled, evictionPolicy, cacheDeserializedValues);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        MapServiceContext mapServiceContext = mock(MapServiceContext.class);

        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(clusterService.getClusterVersion()).thenReturn(Versions.CURRENT_CLUSTER_VERSION);

        MapContainer mapContainer = mock(MapContainer.class);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);
        when(mapContainer.getEvictor()).thenReturn(evictionPolicy == EvictionPolicy.NONE
                ? Evictor.NULL_EVICTOR : mock(Evictor.class));
        when(mapContainer.getMapServiceContext()).thenReturn(mapServiceContext);
        return mapContainer;
    }

    protected MapConfig newMapConfig(boolean perEntryStatsEnabled,
                                     EvictionPolicy evictionPolicy,
                                     CacheDeserializedValues cacheDeserializedValues) {
        MapConfig mapConfig = new MapConfig()
                .setPerEntryStatsEnabled(perEntryStatsEnabled)
                .setCacheDeserializedValues(cacheDeserializedValues);
        mapConfig.getEvictionConfig()
                .setEvictionPolicy(evictionPolicy);
        return mapConfig;
    }
}
