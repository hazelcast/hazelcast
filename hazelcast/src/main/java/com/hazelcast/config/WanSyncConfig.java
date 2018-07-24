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

import java.io.IOException;

/**
 * Configuration object for the WAN sync mechanism.
 *
 * @see WanPublisherConfig
 */
public class WanSyncConfig implements IdentifiedDataSerializable {
    private static final long DEFAULT_CONSISTENCY_CHECK_PERIOD_MILLIS = -1;
    private static final boolean DEFAULT_USE_MERKLE_TREES = false;

    private boolean useMerkleTrees = DEFAULT_USE_MERKLE_TREES;
    private long consistencyCheckPeriodMillis = DEFAULT_CONSISTENCY_CHECK_PERIOD_MILLIS;

    /**
     * Returns if merkle trees should be used to optimise WAN sync.
     * For the check procedure to work properly, the target cluster should
     * support merkle trees and the data structures being synced should be
     * configured with merkle trees enabled both on the source and target cluster.
     * Default value is {@code false}.
     *
     * @return {@code true} if WAN sync should use merkle trees, {@code false}
     * otherwise
     * @see MerkleTreeConfig
     */
    public boolean isUseMerkleTrees() {
        return useMerkleTrees;
    }

    /**
     * Sets if merkle trees should be used to optimise WAN sync.
     * For the check procedure to work properly, the target cluster should
     * support merkle trees and the data structures being synced should be
     * configured with merkle trees enabled both on the source and target cluster.
     * Default value is {@code false}.
     *
     * @param useMerkleTrees if WAN sync should use merkle trees
     * @see MerkleTreeConfig
     */
    public WanSyncConfig setUseMerkleTrees(boolean useMerkleTrees) {
        this.useMerkleTrees = useMerkleTrees;
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
     * Default value is {@code -1}, which means the periodic check is disabled.
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
     * Default value is {@code -1}, which means the periodic check is disabled.
     */
    public void setConsistencyCheckPeriodMillis(long consistencyCheckPeriodMillis) {
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
        out.writeBoolean(useMerkleTrees);
        out.writeLong(consistencyCheckPeriodMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        useMerkleTrees = in.readBoolean();
        consistencyCheckPeriodMillis = in.readLong();
    }

    @Override
    public String toString() {
        return "WanSyncConfig{"
                + "useMerkleTrees=" + useMerkleTrees
                + ", consistencyCheckPeriodMillis=" + consistencyCheckPeriodMillis
                + '}';
    }
}
