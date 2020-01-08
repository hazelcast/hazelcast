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

package com.hazelcast.json.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * A node that describes either a Json array or Json object.
 */
public class JsonSchemaStructNode extends JsonSchemaNode {

    private final List<JsonSchemaNameValue> inners = new ArrayList<JsonSchemaNameValue>();

    public JsonSchemaStructNode(JsonSchemaStructNode parent) {
        super(parent);
    }

    /**
     * Adds a child node to this Json node. It is added as the last item
     * in order.
     *
     * @param description
     */
    public void addChild(JsonSchemaNameValue description) {
        inners.add(description);
    }

    /**
     * Returns a name-value pair for the ith child of this struct.
     * For arrays, it is the simple order index. For attributes,
     * the index is the definition order of the specific attribute
     * within the object. See {@link JsonPattern}.
     *
     * @param i the order of the desired child
     * @return a name value pair
     */
    public JsonSchemaNameValue getChild(int i) {
        return inners.get(i);
    }

    /**
     * The size of this object in terms of items or attributes it contains.
     *
     * @return the number of child nodes
     */
    public int getChildCount() {
        return inners.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        JsonSchemaStructNode that = (JsonSchemaStructNode) o;

        return inners != null ? inners.equals(that.inners) : that.inners == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (inners != null ? inners.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JsonSchemaStructNode{"
                + "inners=" + inners
                + '}';
    }
}
