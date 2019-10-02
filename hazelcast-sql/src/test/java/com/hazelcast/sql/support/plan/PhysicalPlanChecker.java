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

package com.hazelcast.sql.support.plan;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.ProjectPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedMapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;

import java.util.ArrayDeque;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility method which checks physical plan content.
 */
public class PhysicalPlanChecker {

    private final PhysicalNode node;

    private PhysicalPlanChecker(PhysicalNode node) {
        this.node = node;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void checkPlan(PhysicalNode otherNode) {
        assertEquals(node, otherNode);
    }

    public static final class Builder {
        private final ArrayDeque<PhysicalNode> stack = new ArrayDeque<>();

        private Builder() {
            // No-op.
        }

        public Builder addReplicatedMapScan(String mapName) {
            return addReplicatedMapScan(mapName, null, null);
        }

        public Builder addReplicatedMapScan(String mapName, List<Expression> projections) {
            return addReplicatedMapScan(mapName, projections, null);
        }

        public Builder addReplicatedMapScan(String mapName, List<Expression> projections, Expression<Boolean> filter) {
            ReplicatedMapScanPhysicalNode node = new ReplicatedMapScanPhysicalNode(mapName, projections, filter);

            stack.push(node);

            return this;
        }

        public Builder addProject(List<Expression> projections) {
            PhysicalNode upstream = stack.pop();

            ProjectPhysicalNode node = new ProjectPhysicalNode(upstream, projections);

            stack.push(node);

            return this;
        }

        public Builder addFilter(Expression<Boolean> filter) {
            PhysicalNode upstream = stack.pop();

            FilterPhysicalNode node = new FilterPhysicalNode(upstream, filter);

            stack.push(node);

            return this;
        }

        public Builder addRoot() {
            PhysicalNode upstream = stack.pop();

            RootPhysicalNode node = new RootPhysicalNode(upstream);

            stack.push(node);

            return this;
        }

        public PhysicalPlanChecker build() {
            PhysicalNode node = stack.pop();

            assertTrue(stack.isEmpty());

            return new PhysicalPlanChecker(node);
        }
    }
}
