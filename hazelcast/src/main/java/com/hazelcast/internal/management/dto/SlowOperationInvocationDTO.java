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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.json.internal.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationLog.Invocation}.
 */
public class SlowOperationInvocationDTO implements JsonSerializable {

    public int id;
    public String operationDetails;
    public long startedAt;
    public int durationMs;

    public SlowOperationInvocationDTO() {
    }

    public SlowOperationInvocationDTO(int id, String operationDetails, long startedAt, int durationMs) {
        this.id = id;
        this.operationDetails = operationDetails;
        this.startedAt = startedAt;
        this.durationMs = durationMs;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("id", id);
        root.add("details", operationDetails);
        root.add("startedAt", startedAt);
        root.add("durationMs", durationMs);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        id = getInt(json, "id");
        operationDetails = getString(json, "details");
        startedAt = getLong(json, "startedAt");
        durationMs = getInt(json, "durationMs");
    }
}
