/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.strategy;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.HashUtil;

/**
 * Default hashing strategy based on Hazelcast partition-id calculation;
 */
public final class DefaultHashingStrategy implements HashingStrategy {
    /**
     * Default singleton instance for the default hashing strategy;
     */
    public static final HashingStrategy INSTANCE = new DefaultHashingStrategy();

    private DefaultHashingStrategy() {
    }

    @Override
    public int hash(Object object,
                    Object partitionKey,
                    ContainerDescriptor containerDescriptor) {
        if (partitionKey instanceof Data) {
            return ((Data) partitionKey).getPartitionHash();
        }

        InternalSerializationService serializationService = (InternalSerializationService) containerDescriptor
                .getNodeEngine().getSerializationService();
        byte[] bytes = serializationService.toBytes(partitionKey);
        return HashUtil.MurmurHash3_x86_32(bytes, HeapData.DATA_OFFSET, bytes.length - HeapData.DATA_OFFSET);
    }
}
