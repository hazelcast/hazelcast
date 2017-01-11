package com.hazelcast.internal.management.dto;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
