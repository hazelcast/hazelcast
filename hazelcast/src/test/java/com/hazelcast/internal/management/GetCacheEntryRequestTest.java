/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.request.GetCacheEntryRequest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetCacheEntryRequestTest extends CacheTestSupport {
    private static final Random random = new Random();

    private TestHazelcastInstanceFactory instanceFactory;
    private HazelcastInstance[] instances;
    private String cacheName = randomName();
    private String value = randomString();

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY}, {OBJECT},
        });
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instances[0];
    }

    protected CacheSimpleConfig getCacheConfig() {
        return new CacheSimpleConfig().setName(cacheName).setInMemoryFormat(inMemoryFormat);
    }

    @Override
    protected void onSetup() {
        Config config = getConfig();
        config.addCacheConfig(getCacheConfig());

        instanceFactory = createHazelcastInstanceFactory(2);
        instances = new HazelcastInstance[2];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = instanceFactory.newHazelcastInstance(config);
        }
    }

    @Override
    protected void onTearDown() {
        instanceFactory.shutdownAll();
    }

    @Test
    public void testGetCacheEntry_string() {
        String key = randomString();

        cacheManager.getCache(cacheName).put(key, value);

        JsonObject result = sendRequestToInstance(instances[0], new GetCacheEntryRequest("string", cacheName, key));
        assertEquals(value, result.get("cacheBrowse_value").asString());
    }

    @Test
    public void testGetCacheEntry_long() {
        long key = random.nextLong();

        cacheManager.getCache(cacheName).put(key, value);

        JsonObject result = sendRequestToInstance(instances[0],
                new GetCacheEntryRequest("long", cacheName, String.valueOf(key)));
        assertEquals(value, result.get("cacheBrowse_value").asString());
    }

    @Test
    public void testGetCacheEntry_integer() {
        int key = random.nextInt();

        cacheManager.getCache(cacheName).put(key, value);

        JsonObject result = sendRequestToInstance(instances[0],
                new GetCacheEntryRequest("integer", cacheName, String.valueOf(key)));
        assertEquals(value, result.get("cacheBrowse_value").asString());
    }

    @Test
    public void testGetCacheEntry_remoteMember() {
        Cache<String, String> cache = cacheManager.getCache(cacheName);

        String key = generateKeyOwnedBy(instances[0]);
        String value = randomString();

        cache.put(key, value);

        JsonObject result = sendRequestToInstance(instances[1], new GetCacheEntryRequest("string", cacheName, key));
        assertEquals(value, result.get("cacheBrowse_value").asString());
    }

    @Test
    public void testGetCacheEntry_missingKey() {
        // ensure CacheConfig already exists on all members
        cacheManager.getCache(cacheName);
        String key = generateKeyOwnedBy(instances[1]);

        JsonObject result = sendRequestToInstance(instances[0], new GetCacheEntryRequest("string", cacheName, key));
        assertNull(result.get("cacheBrowse_value"));
    }

    private JsonObject sendRequestToInstance(HazelcastInstance instance, GetCacheEntryRequest request) {
        ManagementCenterService managementCenterService = getNode(instance).getManagementCenterService();
        JsonObject responseJson = new JsonObject();
        request.writeResponse(managementCenterService, responseJson);
        return (JsonObject) responseJson.get("result");
    }
}
