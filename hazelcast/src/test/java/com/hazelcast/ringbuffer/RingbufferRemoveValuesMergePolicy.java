package com.hazelcast.ringbuffer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;

class RingbufferRemoveValuesMergePolicy
        implements SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes> {

    @Override
    public RingbufferMergeData merge(RingbufferMergeTypes mergingValue, RingbufferMergeTypes existingValue) {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }
}
