/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Map;

/**
 * Provides some functionality for some predicates
 * such as Between, In.
 */
public abstract class AbstractPredicate implements IndexAwarePredicate, DataSerializable {

    protected String attributeName;
    private transient volatile AttributeType attributeType;

    protected AbstractPredicate() {
    }

    protected AbstractPredicate(String attributeName) {
        this.attributeName = attributeName;
    }

    /**
     * Converts givenAttributeValue to the type of entryAttributeValue
     * Good practice: do not invoke this method if entryAttributeValue == null
     *
     * @param entry               map entry on the basis of which the conversion will be executed
     * @param entryAttributeValue attribute value extracted from the entry
     * @param givenAttributeValue given attribute value to be converted
     * @return converted givenAttributeValue
     */
    protected Comparable convert(Map.Entry entry, Comparable entryAttributeValue, Comparable givenAttributeValue) {
        if (givenAttributeValue == null) {
            return null;
        }
        if (givenAttributeValue instanceof IndexImpl.NullObject) {
            return IndexImpl.NULL;
        }
        AttributeType type = attributeType;
        if (type == null) {
            if (entryAttributeValue == null) {
                // we can't convert since we cannot infer the entry's type from a null attribute value.
                // Returning unconverted value is an optimization since the given value will be compared with null.
                return givenAttributeValue;
            }
            type = ((QueryableEntry) entry).getAttributeType(attributeName);
            attributeType = type;
        }

        Class<?> entryAttributeClass = entryAttributeValue != null ? entryAttributeValue.getClass() : null;
        return convert(type, entryAttributeClass, givenAttributeValue);
    }

    private Comparable convert(AttributeType entryAttributeType, Class<?> entryAttributeClass, Comparable givenAttributeValue) {
        if (entryAttributeType == AttributeType.ENUM) {
            // if attribute type is enum, convert given attribute to enum string
            return entryAttributeType.getConverter().convert(givenAttributeValue);
        } else {
            // if given attribute value is already in expected type then there's no need for conversion.
            if (entryAttributeClass != null && entryAttributeClass.isAssignableFrom(givenAttributeValue.getClass())) {
                return givenAttributeValue;
            } else if (entryAttributeType != null) {
                return entryAttributeType.getConverter().convert(givenAttributeValue);
            } else {
                throw new QueryException("Unknown attribute type: " + givenAttributeValue.getClass().getName()
                        + " for attribute: " + attributeName);
            }
        }
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return getIndex(queryContext) != null;
    }

    protected Index getIndex(QueryContext queryContext) {
        return queryContext.getIndex(attributeName);
    }

    protected Object readAttributeValue(Map.Entry entry) {
        QueryableEntry queryableEntry = (QueryableEntry) entry;
        Object attributeValue = queryableEntry.getAttributeValue(attributeName);
        return convertEnumValue(attributeValue);
    }

    protected Object convertEnumValue(Object attributeValue) {
        if (attributeValue != null && attributeValue.getClass().isEnum()) {
            attributeValue = attributeValue.toString();
        }
        return attributeValue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributeName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attributeName = in.readUTF();
    }
}
