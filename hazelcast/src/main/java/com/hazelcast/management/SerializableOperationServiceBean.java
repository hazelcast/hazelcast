/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.spi.OperationService;

import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.OperationServiceMBean}.
 */
public class SerializableOperationServiceBean implements JsonSerializable {


    private int responseQueueSize;
    private int operationExecutorQueueSize;
    private int runningOperationsCount;
    private int remoteOperationCount;
    private long executedOperationCount;
    private long operationThreadCount;

    public SerializableOperationServiceBean() {
    }

    public SerializableOperationServiceBean(OperationService os) {
        responseQueueSize = os.getResponseQueueSize();
        operationExecutorQueueSize = os.getOperationExecutorQueueSize();
        runningOperationsCount = os.getRunningOperationsCount();
        remoteOperationCount = os.getRemoteOperationsCount();
        executedOperationCount = os.getExecutedOperationCount();
        operationThreadCount = os.getPartitionOperationThreadCount();
    }

    public int getResponseQueueSize() {
        return responseQueueSize;
    }

    public void setResponseQueueSize(int responseQueueSize) {
        this.responseQueueSize = responseQueueSize;
    }

    public int getOperationExecutorQueueSize() {
        return operationExecutorQueueSize;
    }

    public void setOperationExecutorQueueSize(int operationExecutorQueueSize) {
        this.operationExecutorQueueSize = operationExecutorQueueSize;
    }

    public int getRunningOperationsCount() {
        return runningOperationsCount;
    }

    public void setRunningOperationsCount(int runningOperationsCount) {
        this.runningOperationsCount = runningOperationsCount;
    }

    public int getRemoteOperationCount() {
        return remoteOperationCount;
    }

    public void setRemoteOperationCount(int remoteOperationCount) {
        this.remoteOperationCount = remoteOperationCount;
    }

    public long getExecutedOperationCount() {
        return executedOperationCount;
    }

    public void setExecutedOperationCount(long executedOperationCount) {
        this.executedOperationCount = executedOperationCount;
    }

    public long getOperationThreadCount() {
        return operationThreadCount;
    }

    public void setOperationThreadCount(long operationThreadCount) {
        this.operationThreadCount = operationThreadCount;
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
