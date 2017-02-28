package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.internal.management.request.GetCacheEntryRequest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetCacheEntryRequestTest extends CacheTestSupport {

    private HazelcastInstance hz;
    private ManagementCenterService managementCenterService;

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hz;
    }

    @Override
    protected void onSetup() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName("test"));
        config.getCacheConfig("test").setStatisticsEnabled(true);
        hz = createHazelcastInstance(config);
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Override
    protected void onTearDown() {
        hz.shutdown();
    }

    @Test
    public void testGetCacheEntry() throws Exception {
        GetCacheEntryRequest request = new GetCacheEntryRequest("string", "test", "1");
        ICacheManager hazelcastCacheManager = hz.getCacheManager();
        ICache<String, String> cache = hazelcastCacheManager.getCache("test");
        cache.put("1", "one");

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);
        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals("one", result.get("cacheBrowse_value").asString());
    }
}
