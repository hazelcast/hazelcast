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

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.JsonUtil.getArray;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationLog}.
 */
public class SlowOperationDTO implements JsonSerializable {

    public String operation;
    public String stackTrace;
    public int totalInvocations;
    public List<SlowOperationInvocationDTO> invocations;

    public SlowOperationDTO() {
    }

    public SlowOperationDTO(String operation, String stackTrace, int totalInvocations,
                            List<SlowOperationInvocationDTO> invocations) {
        this.operation = operation;
        this.stackTrace = stackTrace;
        this.totalInvocations = totalInvocations;
        this.invocations = invocations;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("operation", operation);
        root.add("stackTrace", stackTrace);
        root.add("totalInvocations", totalInvocations);
        JsonArray invocationArray = new JsonArray();
        for (SlowOperationInvocationDTO invocation : invocations) {
            JsonObject json = invocation.toJson();
            if (json != null) {
                invocationArray.add(json);
            }
        }
        root.add("invocations", invocationArray);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        operation = getString(json, "operation");
        stackTrace = getString(json, "stackTrace");
        totalInvocations = getInt(json, "totalInvocations");

        invocations = new ArrayList<SlowOperationInvocationDTO>();
        for (JsonValue jsonValue : getArray(json, "invocations")) {
            SlowOperationInvocationDTO slowOperationInvocationDTO = new SlowOperationInvocationDTO();
            slowOperationInvocationDTO.fromJson(jsonValue.asObject());
            invocations.add(slowOperationInvocationDTO);
        }
    }
}
