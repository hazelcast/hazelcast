package com.hazelcast.ringbuffer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;

class RingbufferMergeIntegerValuesMergePolicy
        implements SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes> {

    @Override
    public RingbufferMergeData merge(RingbufferMergeTypes mergingValue, RingbufferMergeTypes existingValue) {
        final RingbufferMergeData mergingRingbuffer = mergingValue.getDeserializedValue();
        final RingbufferMergeData result = new RingbufferMergeData(mergingRingbuffer.getCapacity());
        RingbufferMergeData existingRingbuffer;
        if (existingValue != null) {
            existingRingbuffer = existingValue.getDeserializedValue();
        } else {
            existingRingbuffer = new RingbufferMergeData(mergingRingbuffer.getCapacity());
        }

        for (Object value : mergingRingbuffer) {
            if (value instanceof Integer) {
                result.add(value);
            }
        }

        for (Object value : existingRingbuffer) {
            if (value instanceof Integer) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }
}
