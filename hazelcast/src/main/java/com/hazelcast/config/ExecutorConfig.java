/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class ExecutorConfig implements DataSerializable {

    public final static int DEFAULT_POOL_SIZE = 8;
    public final static int DEFAULT_QUEUE_CAPACITY = Integer.MAX_VALUE;

    private String name = "default";

    private int poolSize = DEFAULT_POOL_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private boolean statisticsEnabled = true;

    public ExecutorConfig() {
    }

    public ExecutorConfig(String name) {
        this.name = name;
    }

    public ExecutorConfig(String name, int poolSize) {
        this.name = name;
        this.poolSize = poolSize;
    }

    public String getName() {
        return name;
    }

    public ExecutorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @return the poolSize
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @param poolSize the poolSize to set
     */
    public ExecutorConfig setPoolSize(final int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        this.poolSize = poolSize;
        return this;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public ExecutorConfig setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return this;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(poolSize);
        out.writeInt(queueCapacity);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        poolSize = in.readInt();
        queueCapacity = in.readInt();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ExecutorConfig");
        sb.append("{name='").append(name).append('\'');
        sb.append(", poolSize=").append(poolSize);
        sb.append(", queueCapacity=").append(queueCapacity);
        sb.append('}');
        return sb.toString();
    }
}
