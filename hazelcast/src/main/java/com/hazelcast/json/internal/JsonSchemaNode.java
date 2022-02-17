/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.json.internal;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * A node that describes either a JsonValue
 */
public abstract class JsonSchemaNode implements IdentifiedDataSerializable {

    private JsonSchemaStructNode parent;

    public JsonSchemaNode() {
        // no-op
    }
    /**
     * Creates a schema node with given parent
     *
     * @param parent may be null for top element
     */
    public JsonSchemaNode(JsonSchemaStructNode parent) {
        this.parent = parent;
    }


    /**
     * Returns the parent.
     *
     * @return the parent
     */
    public JsonSchemaStructNode getParent() {
        return parent;
    }

    /**
     * Sets the parent of this node. May be null for top element.
     *
     * @param parent
     */
    public void setParent(JsonSchemaStructNode parent) {
        this.parent = parent;
    }

    /**
     * Returns whether this node represents a scalar value.
     *
     * @return false if this is an array or object, true otherwise
     */
    public boolean isTerminal() {
        return false;
    }
}
