/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Shared logic for PartitioningStrategies and classes using parts of their logic.
 */
public final class PartitioningStrategyUtil {
    /**
     * Partitioning strategy that ignores {@link PartitionAware} for keys.
     */
    private static final PartitioningStrategy IDENTITY_PARTITIONING_STRATEGY = v -> v;

    private PartitioningStrategyUtil() { }

    /**
     * Constructs a PartitioningKey depending on the number of attribute values - either returns a singular object
     * or an array of attribute values. Kept in this utility class as a way to centralize this logic.
     *
     * @param keyAttributes - array of key attribute values (e.g. POJO field values)
     * @return single attribute value Object or an Object[] array of attribute values.
     */
    public static Object constructAttributeBasedKey(Object[] keyAttributes) {
        return keyAttributes.length == 1 ? keyAttributes[0] : keyAttributes;
    }

    /**
     * Gets partition for given key specified as key components. Supports
     * {@link AttributePartitioningStrategy} and {@link
     * DefaultPartitioningStrategy}.
     * @return Partition id to which the key belongs or null if the partition
     *         cannot be determined.
     */
    @Nullable
    public static Integer getPartitionIdFromKeyComponents(@Nonnull NodeEngine nodeEngine,
                                                          @Nullable PartitioningStrategy<?> strategy,
                                                          @Nonnull Object[] partitionKeyComponents) {
        assert strategy == null
                || strategy instanceof DefaultPartitioningStrategy
                || strategy instanceof AttributePartitioningStrategy
                : "Unsupported strategy";

        // constructAttributeBasedKey gives needed result also for DefaultPartitioningStrategy (__key = ?)
        Object finalKey = constructAttributeBasedKey(partitionKeyComponents);
        if (finalKey == null) {
            // __key = ? condition with null parameter. The result of comparison will be always NULL.
            // Similar situation is possible for AttributePartitioningStrategy with single attribute
            // - single attribute is not wrapped in an array.
            // We cannot assign meaningful partition, so just disable pruning in this case.
            // It might be possible to use any partition id to speed up execution
            // but this should be a rare case in practice.
            return null;
        }

        if (strategy instanceof AttributePartitioningStrategy) {
            // Mimic calculation performed for IMap put/get operations
            // in AbstractSerializationService.calculatePartitionHash.
            // IMap partitioning strategy is passed there.
            //
            // We cannot pass AttributePartitioningStrategy because we do not have full key object, but
            // IDENTITY_PARTITIONING_STRATEGY on partitionKeyComponents will give the same result
            // as AttributePartitioningStrategy would on a full key.
            // We also cannot use the default calculation because finalKey might be PartitionAware, which
            // we must ignore to be in line with partition calculation for IMap.
            Data keyData = nodeEngine.getSerializationService()
                    .toData(finalKey, IDENTITY_PARTITIONING_STRATEGY);
            return nodeEngine.getPartitionService().getPartitionId(keyData);
        } else {
            // For other IMap strategies we use default calculation
            return nodeEngine.getPartitionService().getPartitionId(finalKey);
        }
    }
}
