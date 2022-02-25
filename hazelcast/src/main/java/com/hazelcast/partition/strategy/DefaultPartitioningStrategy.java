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

package com.hazelcast.partition.strategy;

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.partition.PartitioningStrategy;

/**
 * A {@link PartitioningStrategy} that checks if the key implements {@link PartitionAware}.
 * If so, the {@link PartitionAware#getPartitionKey()} is called. Otherwise null is returned.
 */
@SerializableByConvention
public class DefaultPartitioningStrategy implements PartitioningStrategy {

    @Override
    public Object getPartitionKey(Object key) {
        if (key instanceof PartitionAware) {
            return ((PartitionAware) key).getPartitionKey();
        }
        return null;
    }
}
