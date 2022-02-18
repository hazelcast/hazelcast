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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.AbstractNearCachePreloaderTest;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContext;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContextBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_INVALIDATE_ON_CHANGE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.test.Accessors.getSerializationService;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class ClientMapNearCachePreloaderTest extends AbstractNearCachePreloaderTest<Data, String> {

    protected final File storeFile = new File("nearCache-" + defaultNearCache + ".store").getAbsoluteFile();
    protected final File storeLockFile = new File(storeFile.getName() + ".lock").getAbsoluteFile();

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(DEFAULT_MEMORY_FORMAT,
                DEFAULT_SERIALIZE_KEYS, DEFAULT_INVALIDATE_ON_CHANGE, KEY_COUNT, storeFile.getParent());
    }

    @Override
    protected File getStoreFile() {
        return storeFile;
    }

    @Override
    protected File getStoreLockFile() {
        return storeLockFile;
    }

    @Override
    protected <K, V> DataStructureAdapter<K, V> getDataStructure(NearCacheTestContext<K, V, Data, String> context, String name) {
        IMap<K, V> map = context.nearCacheInstance.getMap(name);
        return new IMapDataStructureAdapter<K, V>(map);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean createNearCacheInstance) {
        Config config = getConfig();

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        IMap<K, V> memberMap = member.getMap(nearCacheConfig.getName());
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<K, V>(memberMap);

        if (createNearCacheInstance) {
            NearCacheTestContextBuilder<K, V, Data, String> contextBuilder = createNearCacheContextBuilder();
            return contextBuilder
                    .setDataInstance(member)
                    .setDataAdapter(dataAdapter)
                    .build();
        }
        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(member))
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> contextBuilder = createNearCacheContextBuilder();
        return contextBuilder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        IMap<K, V> clientMap = client.getMap(nearCacheConfig.getName());

        NearCacheManager nearCacheManager = ((ClientMapProxy) clientMap).getContext()
                .getNearCacheManager(clientMap.getServiceName());
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(nearCacheConfig.getName());

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(clientMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager);
    }
}
