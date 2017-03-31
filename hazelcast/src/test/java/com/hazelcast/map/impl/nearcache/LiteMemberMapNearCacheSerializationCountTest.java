/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCacheSerializationCountTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link IMap} on Hazelcast Lite members.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class LiteMemberMapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public int[] expectedSerializationCounts;

    @Parameter(value = 1)
    public int[] expectedDeserializationCounts;

    @Parameter(value = 2)
    public InMemoryFormat mapInMemoryFormat;

    @Parameter(value = 3)
    public InMemoryFormat nearCacheInMemoryFormat;

    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory(2);

    @Parameters(name = "mapFormat:{2} nearCacheFormat:{3}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, null},
                {new int[]{1, 0, 0}, new int[]{0, 1, 1}, BINARY, BINARY},
                {new int[]{1, 0, 0}, new int[]{0, 1, 0}, BINARY, OBJECT},

                {new int[]{1, 1, 1}, new int[]{1, 1, 1}, OBJECT, null},
                {new int[]{1, 1, 0}, new int[]{1, 1, 1}, OBJECT, BINARY},
                {new int[]{1, 1, 0}, new int[]{1, 1, 0}, OBJECT, OBJECT},
        });
    }

    @Before
    public void setUp() {
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected int[] getExpectedSerializationCounts() {
        return expectedSerializationCounts;
    }

    @Override
    protected int[] getExpectedDeserializationCounts() {
        return expectedDeserializationCounts;
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(createConfig(nearCacheConfig, false));
        HazelcastInstance liteMember = hazelcastFactory.newHazelcastInstance(createConfig(nearCacheConfig, true));

        IMap<K, V> memberMap = member.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMap<K, V> liteMemberMap = liteMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(liteMember);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContext<K, V, Data, String>(
                getSerializationService(member),
                liteMember,
                member,
                new IMapDataStructureAdapter<K, V>(liteMemberMap),
                new IMapDataStructureAdapter<K, V>(memberMap),
                nearCacheConfig,
                true,
                nearCache,
                nearCacheManager);
    }

    protected Config createConfig(NearCacheConfig nearCacheConfig, boolean liteMember) {
        Config config = getConfig()
                .setLiteMember(liteMember);
        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(mapInMemoryFormat)
                .setBackupCount(0)
                .setAsyncBackupCount(0);
        if (nearCacheConfig != null) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(config.getSerializationConfig());
        return config;
    }
}
