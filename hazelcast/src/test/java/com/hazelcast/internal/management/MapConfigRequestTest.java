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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.MapConfigDTO;
import com.hazelcast.internal.management.request.MapConfigRequest;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapConfigRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;
    private String mapName;
    private MapConfigDTO dto;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();

        managementCenterService = getNode(instances[0]).getManagementCenterService();
        mapName = randomMapName();
        dto = new MapConfigDTO(new MapConfig("MapConfigRequestTest"));
    }

    @Test
    public void testGetMapConfig() {
        MapConfigRequest request = new MapConfigRequest(mapName, dto, false);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertFalse(getBoolean(result, "update"));
        assertTrue(getBoolean(result, "hasMapConfig", false));
        final MapConfigDTO adapter = new MapConfigDTO();
        adapter.fromJson(getObject(result, "mapConfig"));
        MapConfig mapConfig = adapter.getConfig();

        assertNotNull(mapConfig);
        assertEquals("default", mapConfig.getName());
    }

    @Test
    public void testUpdateMapConfig() {
        MapConfigRequest request = new MapConfigRequest(mapName, dto, true);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals("success", getString(result, "updateResult"));
    }
}
