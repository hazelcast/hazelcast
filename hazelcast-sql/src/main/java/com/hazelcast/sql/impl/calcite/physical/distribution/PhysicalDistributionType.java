/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.distribution;

/**
 * Type of physical data distribution.
 */
public enum PhysicalDistributionType {
    /**
     * Data set is distributed between nodes, i.e. every tuple is located on exactly one node, but tuples are
     * potentially located on all nodes.
     */
    DISTRIBUTED,

    /**
     * Same as {@link #DISTRIBUTED}, but partitioning columns are known. That is, if the data set "(a, b, c)" is
     * partitioned by column "a", then all tuples with the same value of "a" will reside on a single node. There may
     * be several partitioning columns, and their order is important. That is, partitioning by (a, b) is not the same
     * as partitioning by (b, a), because in the first case all tuples with the same value of "a" will be on the
     * node, while in the latter case tuples with the same value of "a" may be located on different nodes.
     */
    DISTRIBUTED_PARTITIONED,

    /**
     * The whole data set is located on all nodes. That is, if there are N nodes, there will be N copies of the
     * data set.
     */
    REPLICATED,

    /**
     * The whole data set is located on exactly one node.
     */
    SINGLETON,

    /**
     * Abstract unknown distribution. Rel nodes start in this mode, but must be converted to specific distribution
     * eventually.
     */
    ANY
}
