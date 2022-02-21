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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CollectionMergeTypes;

/**
 * Implementation of {@link CollectionMergeTypes}.
 *
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public class CollectionMergingValueImpl<V> extends AbstractCollectionMergingValueImpl<V, CollectionMergingValueImpl<V>>
        implements CollectionMergeTypes<V> {

    public CollectionMergingValueImpl() {
    }

    public CollectionMergingValueImpl(SerializationService serializationService) {
        super(serializationService);
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.COLLECTION_MERGING_VALUE;
    }
}
