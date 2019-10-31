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

package com.hazelcast.internal.management.dto;

import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getLong;

/**
 * A Serializable DTO for {@link com.hazelcast.internal.jmx.OperationServiceMBean}.
 */
public class OperationServiceDTO implements JsonSerializable {

    public int responseQueueSize;
    public int operationExecutorQueueSize;
    public int runningOperationsCount;
    public int remoteOperationCount;
    public long executedOperationCount;
    public long operationThreadCount;

    public OperationServiceDTO() {
    }

    public OperationServiceDTO(OperationServiceImpl os) {
        responseQueueSize = os.getResponseQueueSize();
        operationExecutorQueueSize = os.getOperationExecutorQueueSize();
        runningOperationsCount = os.getRunningOperationsCount();
        remoteOperationCount = os.getRemoteOperationsCount();
        executedOperationCount = os.getExecutedOperationCount();
        operationThreadCount = os.getPartitionThreadCount();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("responseQueueSize", responseQueueSize);
        root.add("operationExecutorQueueSize", operationExecutorQueueSize);
        root.add("runningOperationsCount", runningOperationsCount);
        root.add("remoteOperationCount", remoteOperationCount);
        root.add("executedOperationCount", executedOperationCount);
        root.add("operationThreadCount", operationThreadCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        responseQueueSize = getInt(json, "responseQueueSize", -1);
        operationExecutorQueueSize = getInt(json, "operationExecutorQueueSize", -1);
        runningOperationsCount = getInt(json, "runningOperationsCount", -1);
        remoteOperationCount = getInt(json, "remoteOperationCount", -1);
        executedOperationCount = getLong(json, "executedOperationCount", -1L);
        operationThreadCount = getLong(json, "operationThreadCount", -1L);
    }
}
