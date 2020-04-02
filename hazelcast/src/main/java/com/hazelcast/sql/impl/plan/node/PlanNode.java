/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.exec.Exec;

/**
 * A single node from the query plan. Represents metadata about a single execution stage, that could be either relational
 * operator (scan, filter, project, etc.), or maintenance operator (root, sender, receiver).
 * <p>
 * Node is converted to {@link Exec} during actual execution.
 * <p>
 * The class extends {@link DataSerializable}, instead of {@link IdentifiedDataSerializable} to simplify testing.
 * <p>
 * Implementations should define hashCode/equals methods.
 */
public interface PlanNode extends DataSerializable {
    /**
     * @return ID of the node.
     */
    int getId();

    /**
     * Visit the node.
     * <p>
     * If there are inputs, they should be visited first.
     *
     * @param visitor Visitor.
     */
    void visit(PlanNodeVisitor visitor);

    /**
     * Get schema associated with the node.
     *
     * @return Schema.
     */
    PlanNodeSchema getSchema();
}
