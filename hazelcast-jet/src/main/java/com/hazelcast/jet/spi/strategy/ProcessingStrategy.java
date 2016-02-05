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

package com.hazelcast.jet.spi.strategy;

import java.io.Serializable;

/**
 * Strategy which determines how data should be passed
 * from one JET container to another;
 */
public enum ProcessingStrategy implements Serializable {
    /**
     * Next chunk will be passed to the next task one by one;
     */
    ROUND_ROBING,
    /**
     * Data will be passed to all accessible vertices of next container;
     */
    BROADCAST,
    /**
     * Data will be passed in accordance object's hash which will be calculated using classes:
     * <p/>
     * {@link com.hazelcast.jet.spi.strategy.HashingStrategy},
     * {@link com.hazelcast.jet.spi.strategy.CalculationStrategy},
     * {@link com.hazelcast.core.PartitioningStrategy}
     */
    PARTITIONING
}
