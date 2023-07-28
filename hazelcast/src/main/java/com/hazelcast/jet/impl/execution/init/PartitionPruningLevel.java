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

package com.hazelcast.jet.impl.execution.init;

import java.util.EnumSet;

/**
 * The level of partition pruning that is required for a DAG to be executed.
 * If member pruning is not possible, {@link PartitionPruningLevel#EMPTY_PRUNING} should be used.
 * Levels are not excluding, and may be combined.
 */
public enum PartitionPruningLevel {
    /**
     * For this level, all partitions must be assigned to all required members
     * were chosen to participate in job execution. It is applicable, if
     * DAG contains at least one partitioned edge with non-constant key.
     */
    ALL_PARTITIONS_REQUIRED;

    public static final EnumSet<PartitionPruningLevel> EMPTY_PRUNING = EnumSet.noneOf(PartitionPruningLevel.class);
}
