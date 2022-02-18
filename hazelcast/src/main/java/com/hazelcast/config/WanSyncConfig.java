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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Configuration object for the WAN sync mechanism.
 *
 * @see WanBatchPublisherConfig
 * @since 3.11
 */
public class WanSyncConfig implements IdentifiedDataSerializable {

    private ConsistencyCheckStrategy consistencyCheckStrategy = ConsistencyCheckStrategy.NONE;

    /**
     * Returns the strategy for checking consistency of data between source and
     * target cluster. Any inconsistency will not be reconciled, it will be
     * merely reported via the usual mechanisms (e.g. statistics, diagnostics).
     * The user must initiate WAN sync to reconcile there differences. For the
     * check procedure to work properly, the target cluster should support the
     * chosen strategy.
     * <p>
     * Default value is {@link ConsistencyCheckStrategy#NONE}, which means the
     * check is disabled.
     */
    public ConsistencyCheckStrategy getConsistencyCheckStrategy() {
        return consistencyCheckStrategy;
    }

    /**
     * Sets the strategy for checking consistency of data between source and
     * target cluster. Any inconsistency will not be reconciled, it will be
     * merely reported via the usual mechanisms (e.g. statistics, diagnostics).
     * The user must initiate WAN sync to reconcile there differences. For the
     * check procedure to work properly, the target cluster should support the
     * chosen strategy.
     * <p>
     * Default value is {@link ConsistencyCheckStrategy#NONE}, which means the
     * check is disabled.
     */
    public WanSyncConfig setConsistencyCheckStrategy(ConsistencyCheckStrategy consistencyCheckStrategy) {
        this.consistencyCheckStrategy = consistencyCheckStrategy;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.WAN_SYNC_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(consistencyCheckStrategy.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        consistencyCheckStrategy = ConsistencyCheckStrategy.getById(in.readByte());
    }

    @Override
    public String toString() {
        return "WanSyncConfig{"
                + "consistencyCheckStrategy=" + consistencyCheckStrategy
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WanSyncConfig that = (WanSyncConfig) o;

        return consistencyCheckStrategy == that.consistencyCheckStrategy;
    }

    @Override
    public int hashCode() {
        return consistencyCheckStrategy != null ? consistencyCheckStrategy.hashCode() : 0;
    }
}
