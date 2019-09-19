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
import com.hazelcast.internal.management.request.ThreadDumpRequest;
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
import static com.hazelcast.internal.util.JsonUtil.getString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadDumpRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();

        managementCenterService = getNode(instances[0]).getManagementCenterService();
    }

    @Test
    public void testThreadDumpRequest() throws Exception {
        ThreadDumpRequest request = new ThreadDumpRequest(false);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertTrue(getBoolean(result, "hasDump"));
        assertNotNull(getString(result, "dump"));
    }

    @Test
    public void testThreadDumpRequest_withDeadlocks() throws Exception {
        ThreadDumpRequest request = new ThreadDumpRequest(true);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertTrue(getBoolean(result, "hasDump"));
        assertNotNull(getString(result, "dump"));
    }
}
