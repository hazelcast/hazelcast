/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.distribution;

/**
 * Type of physical data distribution.
 */
public enum DistributionType {
    /**
     * Abstract unknown distribution. Rel nodes start in this mode, but must be converted to specific distribution
     * during physical planning.
     */
    ANY,

    /**
     * Data set is partitioned between nodes. Each tuple is located on exactly one node.
     */
    PARTITIONED,

    /**
     * The whole data set is located on all nodes. That is, if there are N nodes, there will be N copies of the
     * data set.
     */
    REPLICATED,

    /**
     * Data set is located on the root node.
     */
    ROOT
}
