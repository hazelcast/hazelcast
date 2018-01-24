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

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.SplitBrainAwareDataContainer;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.config.CardinalityEstimatorConfig.DEFAULT_ASYNC_BACKUP_COUNT;
import static com.hazelcast.config.CardinalityEstimatorConfig.DEFAULT_SYNC_BACKUP_COUNT;
import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;

public class CardinalityEstimatorContainer
        implements SplitBrainAwareDataContainer<String, HyperLogLog, HyperLogLog>,
                   IdentifiedDataSerializable {

    HyperLogLog hll;

    private int backupCount;

    private int asyncBackupCount;

    public CardinalityEstimatorContainer() {
        this(DEFAULT_SYNC_BACKUP_COUNT, DEFAULT_ASYNC_BACKUP_COUNT);
    }

    public CardinalityEstimatorContainer(int backupCount, int asyncBackupCount) {
        this.hll = new HyperLogLogImpl();
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
    }

    public void add(long hash) {
        hll.add(hash);
    }

    public long estimate() {
        return hll.estimate();
    }

    public int getBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    @Override
    public HyperLogLog merge(SplitBrainMergeEntryView<String, HyperLogLog> mergingEntry,
                                               SplitBrainMergePolicy mergePolicy) {
        String name = mergingEntry.getKey();
        if (hll.estimate() != 0) {
            SplitBrainMergeEntryView<String, HyperLogLog> existing =
                    createSplitBrainMergeEntryView(name, hll);
            HyperLogLog newValue = mergePolicy.merge(mergingEntry, existing);
            if (newValue != null && !newValue.equals(hll)) {
                setValue(newValue);
                return hll;
            }
        } else {
            HyperLogLog newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue != null) {
                setValue(newValue);
                return hll;
            }
        }

        return null;
    }

    public void setValue(HyperLogLog hll) {
        this.hll = hll;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(hll);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        hll = in.readObject();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CardinalityEstimatorDataSerializerHook.CARDINALITY_EST_CONTAINER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CardinalityEstimatorContainer that = (CardinalityEstimatorContainer) o;

        return hll.equals(that.hll);
    }

    @Override
    public int hashCode() {
        return hll.hashCode();
    }

}
