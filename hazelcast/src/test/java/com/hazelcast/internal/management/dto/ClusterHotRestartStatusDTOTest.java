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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterHotRestartStatusDTOTest {

    @Test
    public void testSerialization() {
        Map<String, MemberHotRestartStatus> memberHotRestartStatusMap = new HashMap<String, MemberHotRestartStatus>();
        memberHotRestartStatusMap.put("127.0.0.1:5701", MemberHotRestartStatus.PENDING);
        memberHotRestartStatusMap.put("127.0.0.1:5702", MemberHotRestartStatus.SUCCESSFUL);

        ClusterHotRestartStatusDTO dto = new ClusterHotRestartStatusDTO(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY,
                ClusterHotRestartStatus.IN_PROGRESS, 23, 42, memberHotRestartStatusMap);

        JsonObject json = dto.toJson();
        ClusterHotRestartStatusDTO deserialized = new ClusterHotRestartStatusDTO();
        deserialized.fromJson(json);

        assertEquals(dto.getDataRecoveryPolicy(), deserialized.getDataRecoveryPolicy());
        assertEquals(dto.getHotRestartStatus(), deserialized.getHotRestartStatus());
        assertEquals(dto.getRemainingValidationTimeMillis(), deserialized.getRemainingValidationTimeMillis());
        assertEquals(dto.getRemainingDataLoadTimeMillis(), deserialized.getRemainingDataLoadTimeMillis());
        assertEquals(dto.getMemberHotRestartStatusMap(), deserialized.getMemberHotRestartStatusMap());
    }
}
