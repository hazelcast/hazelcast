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

package com.hazelcast.spi.merge;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Abstract implementation of {@link SplitBrainMergePolicy} for the out-of-the-box merge policies.
 * <p>
 * Doesn't save the injected {@link SerializationService}, since it's not needed by any out-of-the-box merge policy.
 *
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
abstract class AbstractSplitBrainMergePolicy implements SplitBrainMergePolicy, IdentifiedDataSerializable {

    protected void checkInstanceOf(MergeDataHolder dataHolder, Class<?> clazz) {
        if (dataHolder != null && !clazz.isInstance(dataHolder)) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int getFactoryId() {
        return SplitBrainMergePolicyDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }
}
