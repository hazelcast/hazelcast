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

package com.hazelcast.jet2.impl;

import java.io.Serializable;

class Payload implements Serializable {

    private final String engineName;
    private final long executionId;
    private final int vertexId;
    private final int ordinal;
    private final Object item;
    private int partitionId;

    public Payload(String engineName, long executionId, int vertexId, int ordinal, Object item) {
        this.engineName = engineName;
        this.executionId = executionId;
        this.vertexId = vertexId;
        this.ordinal = ordinal;
        this.item = item;
    }

    public String getEngineName() {
        return engineName;
    }

    public long getExecutionId() {
        return executionId;
    }

    public int getVertexId() {
        return vertexId;
    }

    public Object getItem() {
        return item;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getOrdinal() {
        return ordinal;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "engineName='" + engineName + '\'' +
                ", executionId=" + executionId +
                ", vertexId=" + vertexId +
                ", ordinal=" + ordinal +
                ", item=" + item +
                ", partitionId=" + partitionId +
                '}';
    }
}
