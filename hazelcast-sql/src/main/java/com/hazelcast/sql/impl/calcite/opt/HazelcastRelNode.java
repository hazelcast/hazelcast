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

package com.hazelcast.sql.impl.calcite.opt;

import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.rel.RelNode;

/**
 * Marker interface for all Hazelcast relational operators.
 */
public interface HazelcastRelNode extends RelNode {
    default HazelcastRelOptCluster getHazelcastCluster() {
        return OptUtils.getCluster(this);
    }

    default int getMemberCount() {
        return getHazelcastCluster().getDistributionTraitDef().getMemberCount();
    }
}
