/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IExecutorService}.
 */
public class ExecutorConfig implements IdentifiedDataSerializable, Versioned {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    public static final int DEFAULT_POOL_SIZE = 16;

    /**
     * Capacity of queue.
     */
    public static final int DEFAULT_QUEUE_CAPACITY = Integer.MAX_VALUE;

    private String name = "default";

    private int poolSize = DEFAULT_POOL_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private boolean statisticsEnabled = true;

    private String quorumName;

    private transient ExecutorConfigReadOnly readOnly;

    public ExecutorConfig() {
    }

    public ExecutorConfig(String name) {
        this.name = name;
    }

    public ExecutorConfig(String name, int poolSize) {
        this.name = name;
        this.poolSize = poolSize;
    }

    public ExecutorConfig(ExecutorConfig config) {
        this.name = config.name;
        this.poolSize = config.poolSize;
        this.queueCapacity = config.queueCapacity;
        this.statisticsEnabled = config.statisticsEnabled;
        this.quorumName = config.quorumName;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public ExecutorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ExecutorConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the executor task.
     *
     * @return the name of the executor task
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the executor task.
     *
     * @param name the name of the executor task
     * @return this executor config instance
     */
    public ExecutorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the number of executor threads per member for the executor.
     *
     * @return the number of executor threads per member for the executor
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the number of executor threads per member for the executor.
     *
     * @param poolSize the number of executor threads per member for the executor
     * @return this executor config instance
     */
    public ExecutorConfig setPoolSize(final int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        this.poolSize = poolSize;
        return this;
    }

    /**
     * Gets the queue capacity of the executor task. 0 means {@code Integer.MAX_VALUE}.
     *
     * @return Queue capacity of the executor task. 0 means {@code Integer.MAX_VALUE}
     */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * Sets the queue capacity of the executor task. 0 means {@code Integer.MAX_VALUE}.
     *
     * @param queueCapacity Queue capacity of the executor task. 0 means {@code Integer.MAX_VALUE}
     * @return this executor config instance
     */
    public ExecutorConfig setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return this;
    }

    /**
     * Gets if statistics gathering is enabled or disabled on the executor task.
     *
     * @return {@code true} if statistics gathering is enabled on the executor task (default), {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics gathering on the executor task.
     *
     * @param statisticsEnabled {@code true} if statistics gathering is enabled on the executor task, {@code false} otherwise
     * @return this executor config instance
     */
    public ExecutorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Returns the quorum name for operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for operations.
     *
     * @param quorumName the quorum name
     * @return the updated configuration
     */
    public ExecutorConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }


    @Override
    public String toString() {
        return "ExecutorConfig{"
                + "name='" + name + '\''
                + ", poolSize=" + poolSize
                + ", queueCapacity=" + queueCapacity
                + ", quorumName=" + quorumName
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.EXECUTOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(poolSize);
        out.writeInt(queueCapacity);
        out.writeBoolean(statisticsEnabled);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeUTF(quorumName);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        poolSize = in.readInt();
        queueCapacity = in.readInt();
        statisticsEnabled = in.readBoolean();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            quorumName = in.readUTF();
        }
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExecutorConfig)) {
            return false;
        }

        ExecutorConfig that = (ExecutorConfig) o;

        if (poolSize != that.poolSize) {
            return false;
        }
        if (queueCapacity != that.queueCapacity) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + poolSize;
        result = 31 * result + queueCapacity;
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        return result;
    }
}
