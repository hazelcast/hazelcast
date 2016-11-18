/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.request.ConsoleRequest;
import com.hazelcast.internal.management.request.GetHotRestartStatusRequest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetHotRestartStatusRequestTest extends HazelcastTestSupport {

    @Test
    public void testGetStatus_withoutEnterprise() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        ClusterHotRestartStatusDTO clusterHotRestartStatus = getClusterHotRestartStatus(hz);

        assertEquals(FULL_RECOVERY_ONLY, clusterHotRestartStatus.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatus.UNKNOWN, clusterHotRestartStatus.getHotRestartStatus());
        assertEquals(-1, clusterHotRestartStatus.getRemainingValidationTimeMillis());
        assertEquals(-1, clusterHotRestartStatus.getRemainingDataLoadTimeMillis());
        assertTrue(clusterHotRestartStatus.getMemberHotRestartStatusMap().isEmpty());
    }

    public static ClusterHotRestartStatusDTO getClusterHotRestartStatus(HazelcastInstance hz) throws Exception {
        ManagementCenterService managementCenterService = getNode(hz).getManagementCenterService();
        ConsoleRequest request = new GetHotRestartStatusRequest();
        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);
        return (ClusterHotRestartStatusDTO) request.readResponse(jsonObject);
    }
}
