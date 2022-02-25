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
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.Collections;
import java.util.Map;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * A DTO for Hot Restart status of cluster and all members.
 */
public class ClusterHotRestartStatusDTO implements JsonSerializable {

    public enum ClusterHotRestartStatus {
        UNKNOWN, IN_PROGRESS, FAILED, SUCCEEDED
    }

    public enum MemberHotRestartStatus {
        PENDING, LOAD_IN_PROGRESS, SUCCESSFUL, FAILED
    }

    private HotRestartClusterDataRecoveryPolicy dataRecoveryPolicy;
    private ClusterHotRestartStatus hotRestartStatus;
    private long remainingValidationTimeMillis;
    private long remainingDataLoadTimeMillis;
    private Map<String, MemberHotRestartStatus> memberHotRestartStatusMap;

    public ClusterHotRestartStatusDTO() {
        this(FULL_RECOVERY_ONLY, ClusterHotRestartStatus.UNKNOWN, -1, -1,
                Collections.<String, MemberHotRestartStatus>emptyMap());
    }

    public ClusterHotRestartStatusDTO(HotRestartClusterDataRecoveryPolicy dataRecoveryPolicy,
                                      ClusterHotRestartStatus hotRestartStatus,
                                      long remainingValidationTimeMillis,
                                      long remainingDataLoadTimeMillis,
                                      Map<String, MemberHotRestartStatus> memberHotRestartStatusMap) {
        isNotNull(dataRecoveryPolicy, "dataRecoveryPolicy");
        isNotNull(hotRestartStatus, "hotRestartStatus");
        isNotNull(memberHotRestartStatusMap, "memberHotRestartStatusMap");
        this.dataRecoveryPolicy = dataRecoveryPolicy;
        this.hotRestartStatus = hotRestartStatus;
        this.remainingValidationTimeMillis = remainingValidationTimeMillis;
        this.remainingDataLoadTimeMillis = remainingDataLoadTimeMillis;
        this.memberHotRestartStatusMap = memberHotRestartStatusMap;
    }

    public HotRestartClusterDataRecoveryPolicy getDataRecoveryPolicy() {
        return dataRecoveryPolicy;
    }

    public ClusterHotRestartStatus getHotRestartStatus() {
        return hotRestartStatus;
    }

    public long getRemainingValidationTimeMillis() {
        return remainingValidationTimeMillis;
    }

    public long getRemainingDataLoadTimeMillis() {
        return remainingDataLoadTimeMillis;
    }

    public Map<String, MemberHotRestartStatus> getMemberHotRestartStatusMap() {
        return memberHotRestartStatusMap;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("dataRecoveryPolicy", dataRecoveryPolicy.toString());
        root.add("hotRestartStatus", hotRestartStatus.toString());
        root.add("remainingValidationTimeMillis", remainingValidationTimeMillis);
        root.add("remainingDataLoadTimeMillis", remainingDataLoadTimeMillis);

        JsonArray memberStatuses = new JsonArray();
        for (Map.Entry<String, MemberHotRestartStatus> entry : memberHotRestartStatusMap.entrySet()) {
            String member = entry.getKey();
            MemberHotRestartStatus status = entry.getValue();
            JsonObject pair = new JsonObject();
            pair.add("member", member);
            pair.add("status", status.toString());
            memberStatuses.add(pair);
        }
        root.add("memberHotRestartStatuses", memberStatuses);

        return root;
    }

    @Override
    public void fromJson(JsonObject root) {
        String dataRecoveryPolicyStr = root.getString("dataRecoveryPolicy", FULL_RECOVERY_ONLY.toString());
        dataRecoveryPolicy = HotRestartClusterDataRecoveryPolicy.valueOf(dataRecoveryPolicyStr);

        String hotRestartStatusStr = root.getString("hotRestartStatus", ClusterHotRestartStatus.UNKNOWN.toString());
        hotRestartStatus = ClusterHotRestartStatus.valueOf(hotRestartStatusStr);

        remainingValidationTimeMillis = root.getLong("remainingValidationTimeMillis", -1);
        remainingDataLoadTimeMillis = root.getLong("remainingDataLoadTimeMillis", -1);

        JsonArray memberStatuses = (JsonArray) root.get("memberHotRestartStatuses");
        memberHotRestartStatusMap = createHashMap(memberStatuses.size());
        for (JsonValue value : memberStatuses) {
            JsonObject memberStatus = (JsonObject) value;
            String member = memberStatus.getString("member", "<unknown>");
            MemberHotRestartStatus status = MemberHotRestartStatus
                    .valueOf(memberStatus.getString("status", MemberHotRestartStatus.PENDING.toString()));
            memberHotRestartStatusMap.put(member, status);
        }
    }
}
