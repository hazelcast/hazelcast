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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Preconditions;

import java.io.IOException;

/**
 * Configuration object for the WAN sync mechanism.
 *
 * @see WanPublisherConfig
 * @since 3.11
 */
public class WanSyncConfig implements IdentifiedDataSerializable {
    private static final long DEFAULT_CONSISTENCY_CHECK_PERIOD_MILLIS = 0;

    private ConsistencyCheckStrategy consistencyCheckStrategy = ConsistencyCheckStrategy.NONE;
    private long consistencyCheckPeriodMillis = DEFAULT_CONSISTENCY_CHECK_PERIOD_MILLIS;

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

    /**
     * Returns the period (in milliseconds) for checking consistency of data
     * between source and target cluster.
     * Any inconsistency will not be reconciled, it will be merely reported via
     * the usual mechanisms (e.g. statistics, diagnostics). The user must
     * initiate WAN sync to reconcile there differences.
     * For the check procedure to work properly, the target cluster should
     * support merkle trees and the data structures being synced should be
     * configured with merkle trees enabled both on the source and target cluster.
     * Default value is {@code 0}, which means the periodic check is disabled.
     * The period must not be negative.
     */
    public long getConsistencyCheckPeriodMillis() {
        return consistencyCheckPeriodMillis;
    }

    /**
     * Sets the period (in milliseconds) for checking consistency of data
     * between source and target cluster.
     * Any inconsistency will not be reconciled, it will be merely reported via
     * the usual mechanisms (e.g. statistics, diagnostics). The user must
     * initiate WAN sync to reconcile there differences.
     * For the check procedure to work properly, the target cluster should
     * support merkle trees and the data structures being synced should be
     * configured with merkle trees enabled both on the source and target cluster.
     * Default value is {@code 0}, which means the periodic check is disabled.
     * The period must not be negative.
     *
     * @param consistencyCheckPeriodMillis the updated period (in milliseconds)
     * @throws IllegalArgumentException if the provided period is negative
     */
    public void setConsistencyCheckPeriodMillis(long consistencyCheckPeriodMillis) {
        Preconditions.checkNotNegative(consistencyCheckPeriodMillis,
                "Provided consistencyCheckPeriodMillis must not be negative");
        this.consistencyCheckPeriodMillis = consistencyCheckPeriodMillis;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.WAN_SYNC_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(consistencyCheckStrategy.getId());
        out.writeLong(consistencyCheckPeriodMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        consistencyCheckStrategy = ConsistencyCheckStrategy.getById(in.readByte());
        consistencyCheckPeriodMillis = in.readLong();
    }

    @Override
    public String toString() {
        return "WanSyncConfig{"
                + "consistencyCheckStrategy=" + consistencyCheckStrategy
                + ", consistencyCheckPeriodMillis=" + consistencyCheckPeriodMillis
                + '}';
    }
}
