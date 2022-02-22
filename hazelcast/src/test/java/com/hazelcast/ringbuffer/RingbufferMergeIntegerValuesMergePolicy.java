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

package com.hazelcast.ringbuffer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;

class RingbufferMergeIntegerValuesMergePolicy
        implements SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> {

    @Override
    public RingbufferMergeData merge(RingbufferMergeTypes mergingValue, RingbufferMergeTypes existingValue) {
        final RingbufferMergeData mergingRingbuffer = mergingValue.getValue();
        final RingbufferMergeData result = new RingbufferMergeData(mergingRingbuffer.getCapacity());
        RingbufferMergeData existingRingbuffer;
        if (existingValue != null) {
            existingRingbuffer = existingValue.getValue();
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
