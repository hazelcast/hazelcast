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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.request.ForceStartNodeRequest;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ForceStartNodeRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testForceStart_fails_withNoEnterprise() throws Exception {
        ForceStartNodeRequest request = new ForceStartNodeRequest();

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals(ForceStartNodeRequest.FAILED_RESULT, JsonUtil.getString(result, "result"));
    }
}
