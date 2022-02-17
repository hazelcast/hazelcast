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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.Extractable;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.AbstractJsonGetter;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;
import static com.hazelcast.query.impl.IndexUtils.canonicalizeAttribute;
import static com.hazelcast.query.impl.predicates.PredicateUtils.isNull;

/**
 * Provides base features for predicates, such as extraction and conversion of the attribute's value.
 * It also handles apply() on MultiResult.
 */
@BinaryInterface
public abstract class AbstractPredicate<K, V> implements Predicate<K, V>, IdentifiedDataSerializable {

    String attributeName;

    private transient volatile AttributeType attributeType;

    protected AbstractPredicate() {
    }

    protected AbstractPredicate(String attributeName) {
        this.attributeName = canonicalizeAttribute(attributeName);
    }

    @Override
    public boolean apply(Map.Entry<K, V> mapEntry) {
        Object attributeValue = readAttributeValue(mapEntry);
        if (attributeValue instanceof MultiResult) {
            return applyForMultiResult((MultiResult) attributeValue);
        } else if (attributeValue instanceof Collection || attributeValue instanceof Object[]) {
            throw new IllegalArgumentException(String.format("Cannot use %s predicate with an array or a collection attribute",
                    getClass().getSimpleName()));
        }
        return convertAndApplyForSingleAttributeValue(attributeValue);
    }

    private boolean applyForMultiResult(MultiResult result) {
        List results = result.getResults();
        for (Object o : results) {
            Comparable entryValue = (Comparable) o;
            // it's enough if there's only one result in the MultiResult that satisfies the predicate
            boolean satisfied = convertAndApplyForSingleAttributeValue(entryValue);
            if (satisfied) {
                return true;
            }
        }
        return false;
    }

    private boolean convertAndApplyForSingleAttributeValue(Object attributeValue) {
        if (attributeValue instanceof JsonValue) {
            if (attributeValue == NonTerminalJsonValue.INSTANCE) {
                return false;
            }
            attributeValue = AbstractJsonGetter.convertFromJsonValue((JsonValue) attributeValue);
        }
        if (attributeValue instanceof Comparable || attributeValue == null) {
            return applyForSingleAttributeValue((Comparable) attributeValue);
        }
        if (attributeValue instanceof PortableGenericRecord) {
            ClassDefinition classDefinition = ((PortableGenericRecord) attributeValue).getClassDefinition();
            throw new QueryException(attributeName + " field can not be compared, because "
                    + "the user class could not be constructed. ClassDefinition " + classDefinition);
        }
        throw new QueryException(attributeName + " field can not be compared, "
                + "because it does not implement Comparable interface. Class " + attributeValue.getClass());
    }

    protected abstract boolean applyForSingleAttributeValue(Comparable attributeValue);

    /**
     * Converts givenAttributeValue to the type of entryAttributeValue
     * Good practice: do not invoke this method if entryAttributeValue == null
     *
     * @param entryAttributeValue attribute value extracted from the entry
     * @param givenAttributeValue given attribute value to be converted
     * @return converted givenAttributeValue
     */
    protected Comparable convert(Comparable entryAttributeValue, Comparable givenAttributeValue) {
        if (isNull(givenAttributeValue)) {
            return givenAttributeValue;
        }

        AttributeType type = attributeType;
        if (type == null) {
            if (entryAttributeValue == null) {
                // we can't convert since we cannot infer the entry's type from a null attribute value.
                // Returning unconverted value is an optimization since the given value will be compared with null.
                return givenAttributeValue;
            }
            type = QueryableEntry.extractAttributeType(entryAttributeValue);
            attributeType = type;
        }

        return convert(type, entryAttributeValue, givenAttributeValue);
    }

    private Comparable convert(AttributeType entryAttributeType, Comparable entryAttributeValue, Comparable givenAttributeValue) {

        Class<?> entryAttributeClass = entryAttributeValue != null ? entryAttributeValue.getClass() : null;
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

    private Object readAttributeValue(Map.Entry entry) {
        Extractable extractable = (Extractable) entry;
        return extractable.getAttributeValue(attributeName);
    }

    Object convertEnumValue(Object attributeValue) {
        if (attributeValue != null && attributeValue.getClass().isEnum()) {
            attributeValue = attributeValue.toString();
        }
        return attributeValue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributeName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attributeName = in.readString();
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractPredicate)) {
            return false;
        }

        AbstractPredicate<?, ?> that = (AbstractPredicate<?, ?>) o;
        if (!that.canEqual(this)) {
            return false;
        }
        return attributeName != null ? attributeName.equals(that.attributeName) : that.attributeName == null;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean canEqual(Object other) {
        return (other instanceof AbstractPredicate);
    }

    @Override
    public int hashCode() {
        return attributeName != null ? attributeName.hashCode() : 0;
    }

}
