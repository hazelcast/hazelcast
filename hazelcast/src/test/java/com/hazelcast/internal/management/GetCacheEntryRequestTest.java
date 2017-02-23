package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.internal.management.request.GetCacheEntryRequest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetCacheEntryRequestTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("test"));
        config.getCacheConfig("test").setStatisticsEnabled(true);
        hz = createHazelcastInstance(config);
        HazelcastServerCachingProvider.createCachingProvider(hz);
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testGetCacheEntry() throws Exception {
        GetCacheEntryRequest request = new GetCacheEntryRequest("string","test","1");
        ICacheManager hazelcastCacheManager = hz.getCacheManager();
        ICache<String, String> cache = hazelcastCacheManager.getCache("test");
        cache.put("1","one");

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);
        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals("one",result.get("cacheBrowse_value").asString());
    }

}
