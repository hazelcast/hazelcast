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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.json.internal.JsonDataSerializerHook.JSON_SCHEMA_STRUCT_NODE;

/**
 * A node that describes either a Json array or Json object.
 */
public class JsonSchemaStructNode extends JsonSchemaNode {

    private List<JsonSchemaNameValue> inners = new ArrayList<>();

    public JsonSchemaStructNode() {
        // No-op.
    }

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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(inners.size());
        for (int i = 0; i < inners.size(); i++) {
            inners.get(i).writeData(out);
        }
        // Don't serialize parent node from superclass to avoid cyclic dependency
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int innersSize = in.readInt();
        inners = new ArrayList<>(innersSize);
        for (int i = 0; i < innersSize; ++i) {
            JsonSchemaNameValue nameValue = new JsonSchemaNameValue();
            nameValue.readData(in);
            nameValue.getValue().setParent(this);
            inners.add(nameValue);
        }
    }

    @Override
    public int getFactoryId() {
        return JsonDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JSON_SCHEMA_STRUCT_NODE;
    }

}
