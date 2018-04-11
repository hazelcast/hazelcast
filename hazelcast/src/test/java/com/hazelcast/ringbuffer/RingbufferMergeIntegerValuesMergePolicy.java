package com.hazelcast.ringbuffer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;

class RingbufferMergeIntegerValuesMergePolicy
        implements SplitBrainMergePolicy<RingbufferMergeData<Object>, RingbufferMergeTypes> {

    @Override
    public RingbufferMergeData<Object> merge(RingbufferMergeTypes mergingValue, RingbufferMergeTypes existingValue) {
        final RingbufferMergeData<Object> mergingRingbuffer = mergingValue.getDeserializedValue();
        final RingbufferMergeData<Object> result = new RingbufferMergeData<Object>(mergingRingbuffer.getCapacity());
        RingbufferMergeData<Object> existingRingbuffer;
        if (existingValue != null) {
            existingRingbuffer = existingValue.getDeserializedValue();
        } else {
            existingRingbuffer = new RingbufferMergeData<Object>(mergingRingbuffer.getCapacity());
        }

        for (long seq = mergingRingbuffer.getHeadSequence(); seq <= mergingRingbuffer.getTailSequence(); seq++) {
            final Object value = mergingRingbuffer.read(seq);
            if (value instanceof Integer) {
                result.add(value);
            }
        }

        for (long seq = existingRingbuffer.getHeadSequence(); seq <= existingRingbuffer.getTailSequence(); seq++) {
            final Object value = existingRingbuffer.read(seq);
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
