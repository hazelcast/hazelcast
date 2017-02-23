package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.management.request.GetMapEntryRequest;
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
public class GetMapEntryRequestTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testGetMapEntry() throws Exception {
        GetMapEntryRequest request = new GetMapEntryRequest("string","map","key");
        IMap<String, String> map = hz.getMap("map");
        map.put("key","value");

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);
        JsonObject result = (JsonObject) jsonObject.get("result");
        System.out.println(result);
        assertEquals("value",result.get("browse_value").asString());
        assertEquals("java.lang.String",result.get("browse_class").asString());
    }
}
