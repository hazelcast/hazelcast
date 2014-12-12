/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.AttributePredicate;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.util.Map;

/**
 * Provides some functionality for some predicates
 * such as Between, In.
 */
public abstract class AbstractPredicate implements IndexAwarePredicate, DataSerializable, AttributePredicate {

    protected String attribute;
    private transient volatile AttributeType attributeType;

    protected AbstractPredicate() {
    }

    protected AbstractPredicate(String attribute) {
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }

    public int hashCode() {
        return attribute.hashCode();
    }

    protected Comparable convert(Map.Entry mapEntry, Comparable entryValue, Comparable attributeValue) {
        if (attributeValue == null) {
            return null;
        }
        if (attributeValue instanceof IndexImpl.NullObject) {
            return IndexImpl.NULL;
        }
        AttributeType type = attributeType;
        if (type == null) {
            QueryableEntry queryableEntry = (QueryableEntry) mapEntry;
            type = queryableEntry.getAttributeType(attribute);
            attributeType = type;
        }
        if (type == AttributeType.ENUM) {
            // if attribute type is enum, convert given attribute to enum string
            return type.getConverter().convert(attributeValue);
        } else {
            // if given attribute value is already in expected type then there's no need for conversion.
            if (entryValue != null && entryValue.getClass().isAssignableFrom(attributeValue.getClass())) {
                return attributeValue;
            } else if (type != null) {
                return type.getConverter().convert(attributeValue);
            } else {
                throw new QueryException("Unknown attribute type: " + attributeValue.getClass());
            }
        }
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return queryContext.getIdealIndex(this) != null;
    }

    @Override
    public boolean equals(Object predicate) {
        if (predicate instanceof AbstractPredicate) {
            AbstractPredicate p = (AbstractPredicate) predicate;
            return ValidationUtil.equalOrNull(p.attribute, attribute);
        }
        return false;
    }

    protected Index getIndex(QueryContext queryContext) {
        return queryContext.getIdealIndex(this);
    }

    protected Comparable readAttribute(Map.Entry entry) {
        QueryableEntry queryableEntry = (QueryableEntry) entry;
        Comparable val = queryableEntry.getAttribute(attribute);
        if (val != null && val.getClass().isEnum()) {
            val = val.toString();
        }
        return val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
    }
}
