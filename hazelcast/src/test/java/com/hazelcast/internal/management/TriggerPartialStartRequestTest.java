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
import com.hazelcast.internal.management.request.ConsoleRequest;
import com.hazelcast.internal.management.request.TriggerPartialStartRequest;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.JsonUtil.getString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TriggerPartialStartRequestTest extends HazelcastTestSupport {

    @Test
    public void testTriggerPartialStart_fails_withoutEnterprise() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();

        String result = triggerPartialStart(hz);
        assertEquals(TriggerPartialStartRequest.FAILED_RESULT, result);
    }

    public static String triggerPartialStart(HazelcastInstance hz) throws Exception {
        ManagementCenterService managementCenterService = getNode(hz).getManagementCenterService();
        ConsoleRequest request = new TriggerPartialStartRequest();
        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);
        return String.valueOf(getString(jsonObject, "result"));
    }
}
