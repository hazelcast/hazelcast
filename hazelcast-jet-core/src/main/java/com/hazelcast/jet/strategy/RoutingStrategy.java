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

package com.hazelcast.jet.strategy;

import java.io.Serializable;

/**
 * Determines how data should be passed from producers to consumers
 */
public enum RoutingStrategy implements Serializable {
    /**
     * Next chunk will be sent from producer to the consumer in a round robin fashion
     */
    ROUND_ROBIN,
    /**
     * The output of the producer will be available to all consumers
     */
    BROADCAST,
    /**
     * The output of the producer will be partitioned and the partitions will be allocated
     * between consumers, with the same consumer always receiving the same partitions.
     *
     * {@see HashingStrategy},
     * {@see com.hazelcast.core.PartitioningStrategy}
     */
    PARTITIONED
}
