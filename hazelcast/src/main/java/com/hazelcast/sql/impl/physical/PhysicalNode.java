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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;

/**
 * Physical node.
 */
public interface PhysicalNode extends DataSerializable {
    /**
     * @return ID of the node.
     */
    int getId();

    /**
     * Visit the node.
     *
     * @param visitor Visitor.
     */
    void visit(PhysicalNodeVisitor visitor);

    // TODO: Remove and use physical plan.
    int getInputCount();
    PhysicalNode getInput(int i);
    default PhysicalNode getInput() {
        return getInput(0);
    }
}
