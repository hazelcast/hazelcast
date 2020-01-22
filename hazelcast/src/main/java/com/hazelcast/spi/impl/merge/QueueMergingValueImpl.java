/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.QueueMergeTypes;

/**
 * Implementation of {@link QueueMergeTypes}.
 *
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public class QueueMergingValueImpl<V> extends AbstractCollectionMergingValueImpl<V, QueueMergingValueImpl<V>>
        implements QueueMergeTypes<V> {

    public QueueMergingValueImpl() {
    }

    public QueueMergingValueImpl(SerializationService serializationService) {
        super(serializationService);
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.QUEUE_MERGING_VALUE;
    }
}
